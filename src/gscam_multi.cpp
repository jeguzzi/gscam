// Copyright 2022 Jonathan Bohren, Clyde McQueen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>

#include <iostream>
#include <string>

extern "C" {
#include "gst/app/gstappsink.h"
#include "gst/gst.h"
}

#include "camera_info_manager/camera_info_manager.hpp"
#include "gscam/gscam_multi.hpp"
#include "image_transport/image_transport.hpp"
#include "sensor_msgs/image_encodings.hpp"
#include "sensor_msgs/msg/camera_info.hpp"
#include "sensor_msgs/msg/compressed_image.hpp"
#include "sensor_msgs/msg/image.hpp"

#define ON_DEMAND_AVAILABLE false

namespace gscam {

/* The appsink has received a buffer */
static GstFlowReturn new_sample(GstElement *sink, ROSSink *ros_sink) {
  GstSample *sample;
  /* Retrieve the buffer */
  g_signal_emit_by_name(sink, "pull-sample", &sample);
  if (sample) {
    if (ros_sink->active) {
      ros_sink->publish(sample);
    }
    gst_sample_unref(sample);
    return GST_FLOW_OK;
  }

  return GST_FLOW_ERROR;
}

/* The appsink has received a buffer */
static GstFlowReturn new_preroll(GstElement *sink, ROSSink *ros_sink) {
  gst_app_sink_pull_preroll(GST_APP_SINK(sink));
  return GST_FLOW_OK;
}

static void error_cb(GstBus *bus, GstMessage *msg, GSCamMulti *gscam) {
  GError *err;
  gchar *debug_info;

  /* Print error details on the screen */
  gst_message_parse_error(msg, &err, &debug_info);
  RCLCPP_ERROR(gscam->get_logger(), "Error received from element %s: %s\n",
               GST_OBJECT_NAME(msg->src), err->message);
  g_clear_error(&err);
  g_free(debug_info);
}

bool Sink::init(GstElement *bin) {
  GstElement *queue =
      gst_element_factory_make("queue", ("queue_" + name).c_str());
  valve = gst_element_factory_make("valve", ("valve_" + name).c_str());
  gst_bin_add_many(GST_BIN(cam->pipeline_), queue, valve, bin, NULL);
  GstPad *tee_pad = gst_element_request_pad_simple(cam->tee, "src_%u");
  GstPad *queue_pad = gst_element_get_static_pad(queue, "sink");
  if (gst_pad_link(tee_pad, queue_pad) != GST_PAD_LINK_OK) {
    RCLCPP_ERROR(cam->get_logger(), "Failed linking %s", name.c_str());
    return false;
  }
  g_object_set(valve, "drop", true, nullptr);
  g_object_set(valve, "drop-mode", 1, nullptr);

  auto it = gst_bin_iterate_sinks(GST_BIN(bin));

  GValue item = {
      0,
  };
  gst_iterator_next(it, &item);
  sink = (GstElement *)g_value_get_object(&item);
  g_value_unset(&item);
  gst_iterator_free(it);

  gst_element_link_many(queue, valve, bin, NULL);
  // gst_element_link_many(queue, bin, NULL);
  return true;
}

bool ROSSink::init(GstElement *bin, const GstCaps *caps,
                   const std::string &topic, const std::string &frame_id,
                   const rclcpp::QoS &qos) {
  // RCLCPP_INFO(cam->get_logger(), "ROSSink init");

  GstElement *queue =
      gst_element_factory_make("queue", ("queue_" + name).c_str());
  sink = gst_element_factory_make("appsink", ("sink_" + name).c_str());
  valve = gst_element_factory_make("valve", ("valve_" + name).c_str());
  gst_bin_add_many(GST_BIN(cam->pipeline_), queue, valve, bin, sink, NULL);
  gst_base_sink_set_sync(GST_BASE_SINK(sink), (cam->sync_sink_) ? TRUE : FALSE);
  g_object_set(sink, "emit-signals", TRUE, NULL);
  gst_app_sink_set_caps(GST_APP_SINK(sink), caps);

  GstPad *tee_pad = gst_element_request_pad_simple(cam->tee, "src_%u");
  // g_print("Obtained request pad %s\n", gst_pad_get_name(tee_pad));
  GstPad *queue_pad = gst_element_get_static_pad(queue, "sink");

  gst_app_sink_set_emit_signals((GstAppSink *)sink, TRUE);
  g_signal_connect(sink, "new-sample", G_CALLBACK(new_sample), this);
  g_signal_connect(sink, "new-preroll", G_CALLBACK(new_preroll), this);

  if (gst_pad_link(tee_pad, queue_pad) != GST_PAD_LINK_OK) {
    RCLCPP_ERROR(cam->get_logger(), "Failed linking %s", name.c_str());
    // TODO(Jerome): bool
    return false;
  }
  g_object_set(valve, "drop", true, nullptr);
  g_object_set(sink, "async", false, nullptr);
  gst_element_link_many(queue, valve, bin, sink, NULL);
  g_object_set(sink, "drop", true, nullptr);
  g_object_set(sink, "max-buffers", 1, nullptr);
  // g_object_set(queue, "leaky", TRUE, nullptr);
  g_object_set(queue, "max-size-buffers", 1, nullptr);
  return true;
}

bool Sink::start() {
  // if (gst_pad_link(tee_pad, queue_pad) != GST_PAD_LINK_OK) {
  //   RCLCPP_ERROR(cam->get_logger(), "Failed linking ");
  //   return false;
  // }

  // RCLCPP_INFO(cam->get_logger(), "Linked");
  active = true;

  cam->check_activation();

  // gst_element_send_event(valve, gst_event_new_flush_start());
  // gst_element_send_event(valve, gst_event_new_flush_stop(false));
  g_object_set(valve, "drop", false, nullptr);
  // gst_element_set_locked_state(source, true);
  // gst_element_set_locked_state(sink, false);
  // gst_element_set_state(source, GST_STATE_PLAYING);

  RCLCPP_INFO(cam->get_logger(), "Activated %s", name.c_str());

  return true;
}

bool Sink::stop() {
  active = false;

  if (eos) {
    gst_element_send_event(valve, gst_event_new_eos());
    sleep(1);
    gst_element_set_state(sink, GST_STATE_NULL);
  }
  g_object_set(valve, "drop", true, nullptr);

  // gst_element_set_locked_state(sink, true);
  // gst_element_set_state(source, GST_STATE_PAUSED);

  cam->check_activation();

  RCLCPP_INFO(cam->get_logger(), "Deactivated %s", name.c_str());

  // gst_pad_unlink(tee_pad, queue_pad);
  // RCLCPP_INFO(cam->get_logger(), "stopped");
  return true;
}

void Sink::set_activation(int v) {
  if (v < 0 || v > 2) return;

  ActiveMode value{v};

  if ((value == ActiveMode::ON_DEMAND) && !ON_DEMAND_AVAILABLE) {
    RCLCPP_INFO(cam->get_logger(), "On-demand only supported from ROS 2 Iron");
    return;
  }
  if (activation != value) {
    activation = value;
    check_activation();
  }
}

void Sink::check_activation() {
  bool desired = should_be_active();
  // RCLCPP_INFO(cam->get_logger(), "check sink activation %d %d", desired,
  //             active);
  if (desired != active) {
    if (desired) {
      start();
    } else {
      stop();
    }
  }
}

bool Sink::should_be_active() const { return activation == ActiveMode::ON; }

bool ROSSink::should_be_active() const {
  return activation == ActiveMode::ON ||
         (activation == ActiveMode::ON_DEMAND && subscribers > 0);
}

std::tuple<int, int> get_image_size(GstElement *sink) {
  GstPad *pad = gst_element_get_static_pad(sink, "sink");
  const GstCaps *caps = gst_pad_get_current_caps(pad);
  GstStructure *structure = gst_caps_get_structure(caps, 0);
  int w, h;
  gst_structure_get_int(structure, "width", &w);
  gst_structure_get_int(structure, "height", &h);
  return std::make_tuple(w, h);
}

static std::map<const std::string, const std::string> encodings = {
    {"RGB", "rgb8"},      {"BGR", "bgr8"},        {"RGBA", "rgba8"},
    {"BGRA", "bgra8"},    {"GRAY8", "mono8"},     {"UYVY", "yuv422"},
    {"RGB16", "rgb16"},   {"BGR16", "bgr16"},     {"RGBA16", "rgba16"},
    {"BGRA16", "bgra16"}, {"GRAY16_LE", "mono16"}};

bool RawROSSink::init(GstElement *bin, const GstCaps *caps,
                      const std::string &topic, const std::string &frame_id,
                      const rclcpp::QoS &qos) {
  if (!ROSSink::init(bin, caps, topic, frame_id, qos)) return false;

  // RCLCPP_INFO(cam->get_logger(), "RawROSSink init");

  GstStructure *structure = gst_caps_get_structure(caps, 0);
  const char *format = gst_structure_get_string(structure, "format");

  if (encodings.count(std::string(format)) == 0) {
    RCLCPP_ERROR(cam->get_logger(), "Unknown format %s", format);
    return false;
  }

  msg.encoding = encodings[std::string(format)];

  // if (strcmp(format, "RGB") == 0) {
  //   msg.encoding = sensor_msgs::image_encodings::RGB8;
  // } else if (strcmp(format, "GRAY8") == 0) {
  //   msg.encoding = sensor_msgs::image_encodings::MONO8;
  // } else if (strcmp(format, "UYVY") == 0) {
  //   msg.encoding = sensor_msgs::image_encodings::YUV422;
  // } else {
  //   RCLCPP_ERROR(cam->get_logger(), "Unknown format %s", format);
  //   return false;
  // }
  msg.header.frame_id = frame_id;
  msg.is_bigendian = false;
  // RCLCPP_INFO(cam->get_logger(), "create pub on %s", topic.c_str()c);
  pub = cam->create_publisher<sensor_msgs::msg::Image>(topic, qos);
  return true;
}

void RawROSSink::publish(GstSample *sample) {
  GstBuffer *buf = gst_sample_get_buffer(sample);
  GstMemory *memory = gst_buffer_get_memory(buf, 0);
  GstMapInfo info;

  gst_memory_map(memory, &info, GST_MAP_READ);
  gsize &buf_size = info.size;
  guint8 *&buf_data = info.data;

  // GstClockTime bt = gst_element_get_base_time(cam->pipeline_);
  // RCLCPP_INFO(
  //   get_logger(),
  //   "New buffer: timestamp %.6f %lu %lu %.3f",
  //   GST_TIME_AS_USECONDS(buf->timestamp + bt) / 1e6 + time_offset_,
  //   buf->timestamp, bt, time_offset_);

  // Stop on end of stream
  if (!buf) {
    RCLCPP_INFO(cam->get_logger(), "Stream ended.");
    // TODO;
    return;
  }

  // RCLCPP_DEBUG(get_logger(), "Got data.");

  // Get the image width and height
  std::tie(msg.width, msg.height) = get_image_size(sink);
  msg.step =
      msg.width * sensor_msgs::image_encodings::numChannels(msg.encoding);

  // Update header information
  /**
  sensor_msgs::msg::CameraInfo cur_cinfo = camera_info_manager_.getCameraInfo();
  sensor_msgs::msg::CameraInfo::SharedPtr cinfo;
  cinfo.reset(new sensor_msgs::msg::CameraInfo(cur_cinfo));
  if (use_gst_timestamps_) {
    cinfo->header.stamp = rclcpp::Time(GST_TIME_AS_NSECONDS(buf->pts + bt) +
  time_offset_); } else { cinfo->header.stamp = now();
  }
  // RCLCPP_INFO(get_logger(), "Image time stamp:
  %.3f",cinfo->header.stamp.toSec()); cinfo->header.frame_id = frame_id_;
  **/
  // Complain if the returned buffer is smaller than we expect
  //

  const unsigned expected_frame_size = msg.height * msg.step;

  if (buf_size < expected_frame_size) {
    RCLCPP_WARN_STREAM(
        cam->get_logger(),
        "GStreamer image buffer underflow: Expected frame to be "
            << expected_frame_size << " bytes but got only " << buf_size
            << " bytes. (make sure frames are correctly encoded)");
  }

  msg.header.stamp = cam->get_time(buf->pts);
  msg.data.resize(expected_frame_size);

  // Copy only the data we received
  // Since we're publishing shared pointers, we need to copy the image so
  // we can free the buffer allocated by gstreamer

  std::copy(buf_data, (buf_data) + (buf_size), msg.data.begin());
  pub->publish(msg);
  cam->publish_info(buf->pts);

  // Release the buffer
  if (buf) {
    gst_memory_unmap(memory, &info);
    gst_memory_unref(memory);
  }
}

bool JPEGROSSink::init(GstElement *bin, const GstCaps *caps,
                       const std::string &topic, const std::string &frame_id,
                       const rclcpp::QoS &qos) {
  if (!ROSSink::init(bin, caps, topic, frame_id, qos)) return false;
  msg.header.frame_id = frame_id;
  msg.format = "jpeg";
  pub = cam->create_publisher<sensor_msgs::msg::CompressedImage>(topic, qos);
  return true;
}

void JPEGROSSink::publish(GstSample *sample) {
  GstBuffer *buf = gst_sample_get_buffer(sample);
  GstMemory *memory = gst_buffer_get_memory(buf, 0);
  GstMapInfo info;

  gst_memory_map(memory, &info, GST_MAP_READ);
  gsize &buf_size = info.size;
  guint8 *&buf_data = info.data;

  // GstClockTime bt = gst_element_get_base_time(cam->pipeline_);

  // Stop on end of stream
  if (!buf) {
    RCLCPP_INFO(cam->get_logger(), "Stream ended.");
    // TODO;
    return;
  }

  // RCLCPP_DEBUG(get_logger(), "Got data.");

  // Get the image width and height
  // update_size();

  msg.header.stamp = cam->get_time(buf->pts);
  msg.data.resize(buf_size);
  std::copy(buf_data, (buf_data) + (buf_size), msg.data.begin());
  pub->publish(msg);
  cam->publish_info(buf->pts);

  // Release the buffer
  gst_memory_unmap(memory, &info);
  gst_memory_unref(memory);
}

bool FFMPEGROSSink::init(GstElement *bin, const GstCaps *caps,
                       const std::string &topic, const std::string &frame_id,
                       const rclcpp::QoS &qos) {
  if (!ROSSink::init(bin, caps, topic, frame_id, qos)) return false;
  msg.header.frame_id = frame_id;
  pub = cam->create_publisher<ffmpeg_image_transport_msgs::msg::FFMPEGPacket>(
      topic, qos);
  return true;
}

void FFMPEGROSSink::publish(GstSample *sample) {
  GstBuffer *buf = gst_sample_get_buffer(sample);
  GstMemory *memory = gst_buffer_get_memory(buf, 0);
  GstMapInfo info;

  gst_memory_map(memory, &info, GST_MAP_READ);
  gsize &buf_size = info.size;
  guint8 *&buf_data = info.data;

  // GstClockTime bt = gst_element_get_base_time(cam->pipeline_);

  // Stop on end of stream
  if (!buf) {
    RCLCPP_INFO(cam->get_logger(), "Stream ended.");
    // TODO;
    return;
  }

  // RCLCPP_INFO(cam->get_logger(), "Got data: %d", buf_size);

  // Get the image width and height
  std::tie(msg.width, msg.height) = get_image_size(sink);
  msg.header.stamp = cam->get_time(buf->pts);
  msg.pts = pts++;
  msg.flags = 1;
  msg.data.resize(buf_size);
  std::copy(buf_data, (buf_data) + (buf_size), msg.data.begin());
  pub->publish(msg);
  cam->publish_info(buf->pts);

  // Release the buffer
  gst_memory_unmap(memory, &info);
  gst_memory_unref(memory);
}

GSCamMulti::GSCamMulti(const rclcpp::NodeOptions &options)
    : rclcpp::Node(
          "GSCamMulti_publisher",
          rclcpp::NodeOptions(options).allow_undeclared_parameters(true)),
      gsconfig_(""),
      pipeline_(NULL),
      tee(NULL),
      active(false),
      camera_info_manager_(this),
      stop_signal_(false),
      last_pts(0),
      sinks() {
  // declare_parameter("raw", 0);
  // declare_parameter("jpeg", 0);
  // declare_parameter("h264", 0);

  if (!configure()) {
    RCLCPP_FATAL(get_logger(), "Failed to configure GSCamMulti!");
  }

  init();
  pipeline_thread_ = std::thread([this]() { run(); });

  // run();

  callback_handle_ = add_on_set_parameters_callback(
      std::bind(&GSCamMulti::parametersCallback, this, std::placeholders::_1));
}

static GstStaticCaps raw_caps = GST_STATIC_CAPS("video/x-raw");
static GstStaticCaps h264_caps = GST_STATIC_CAPS("video/x-h264");
static GstStaticCaps h265_caps = GST_STATIC_CAPS("video/x-h265");
static GstStaticCaps jpeg_caps = GST_STATIC_CAPS("image/jpeg");

void GSCamMulti::add_ros_sink(const std::string &name, std::string config,
                              std::string caps_, std::string topic) {
  RCLCPP_INFO(get_logger(), "add_ros_sink %s %s %s %s", name.c_str(),
              config.c_str(), caps_.c_str(), topic.c_str());
  GError *err = NULL;
  GstCaps *caps = gst_caps_from_string(caps_.c_str());

  if (!caps || err != NULL) {
    RCLCPP_ERROR(get_logger(), "Could not parse caps from %s", caps_.c_str());
    return;
  }

  GstElement *bin = gst_parse_bin_from_description(config.c_str(), true, &err);

  if (!bin) {
    RCLCPP_ERROR(get_logger(), "Could not parse bin from %s", config.c_str());
    return;
  }

  std::shared_ptr<ROSSink> ros_sink = nullptr;

  if (gst_caps_is_always_compatible(caps, gst_static_caps_get(&raw_caps))) {
    ros_sink = std::make_shared<RawROSSink>(this, name);
    if (topic.empty()) {
      topic = "image_raw";
    }
  } else if (gst_caps_is_always_compatible(caps,
                                           gst_static_caps_get(&jpeg_caps))) {
    ros_sink = std::make_shared<JPEGROSSink>(this, name);
    if (topic.empty()) {
      topic = "image_raw/compressed";
    }
  } else if (gst_caps_is_always_compatible(caps,
                                           gst_static_caps_get(&h264_caps))) {
    if (topic.empty()) {
      topic = "image_raw/ffmpeg";
    }
    ros_sink = std::make_shared<FFMPEGROSSink>(this, name, "libx264");
  } else if (gst_caps_is_always_compatible(caps,
                                           gst_static_caps_get(&h265_caps))) {
    if (topic.empty()) {
      topic = "image_raw/ffmpeg";
    }
    ros_sink = std::make_shared<FFMPEGROSSink>(this, name, "libx265");
  } else {
    RCLCPP_ERROR(get_logger(), "Unknown media format in caps %s",
                 caps_.c_str());
    return;
  }
  const auto qos =
      use_sensor_data_qos_ ? rclcpp::SensorDataQoS() : rclcpp::QoS{1};
  if (ros_sink->init(bin, caps, topic, frame_id_, qos)) {
    sinks[name] = ros_sink;
    // declare_parameter(name, 0);
    RCLCPP_INFO(get_logger(), "Added ros sink %s", name.c_str());
  }
}

void GSCamMulti::add_sink(const std::string &name, const std::string &config,
                          bool eos) {
  RCLCPP_INFO(get_logger(), "add_sink %s %s", name.c_str(), config.c_str());

  GError *err = NULL;

  GstElement *bin = gst_parse_bin_from_description(config.c_str(), true, &err);

  if (!bin || err) {
    RCLCPP_ERROR(get_logger(), "Could not parse bin from %s", config.c_str());
    return;
  }

  std::shared_ptr<Sink> sink = std::make_shared<Sink>(this, name);
  if (sink->init(bin)) {
    sinks[name] = sink;
    sink->eos = eos;
    RCLCPP_INFO(get_logger(), "Added sink %s", name.c_str());
  }
}

void GSCamMulti::init_sinks() {
  std::set<std::string> ros_names;
  std::set<std::string> names;

  auto node_parameters_iface = get_node_parameters_interface();
  for (const auto &pair : node_parameters_iface->get_parameter_overrides()) {
    rcl_interfaces::msg::ParameterDescriptor descriptor;
    descriptor.dynamic_typing = true;
    if (!node_parameters_iface->has_parameter(pair.first)) {
      node_parameters_iface->declare_parameter(pair.first, pair.second,
                                               descriptor, true);
      std::string prefix = pair.first.substr(0, pair.first.find("."));
      if (prefix == "sinks") {
        std::string name =
            pair.first.substr(prefix.size() + 1, std::string::npos);
        name = name.substr(0, name.find("."));
        names.insert(name);
      }
      if (prefix == "ros_sinks") {
        std::string name =
            pair.first.substr(prefix.size() + 1, std::string::npos);
        name = name.substr(0, name.find("."));
        ros_names.insert(name);
      }
    }
  }

  for (const auto &name : names) {
    const std::string prefix = "sinks." + name + ".";
    bool eos = false;
    if (has_parameter(prefix + "eos")) {
      eos = get_parameter(prefix + "eos").as_bool();
    }
    add_sink(name, get_parameter(prefix + "config").as_string(), eos);
  }

  for (const auto &name : ros_names) {
    const std::string prefix = "ros_sinks." + name + ".";
    std::string topic = "";
    std::string config = "";
    if (has_parameter(prefix + "config")) {
      config = get_parameter(prefix + "config").as_string();
    }
    if (has_parameter(prefix + "topic")) {
      topic = get_parameter(prefix + "topic").as_string();
    }
    const std::string caps = get_parameter(prefix + "caps").as_string();
    add_ros_sink(name, config, caps, topic);
  }
}

// void GSCamMulti::add_sink(const std::string & name, const std::string &
// config, const std::string & caps) {

// }

rclcpp::Time GSCamMulti::get_time(uint64_t pts) const {
  if (use_gst_timestamps_) {
    const GstClockTime bt = gst_element_get_base_time(pipeline_);
    return rclcpp::Time(GST_TIME_AS_NSECONDS(pts + bt) + time_offset_);
  }
  return now();
}

void GSCamMulti::publish_info(uint64_t pts) {
  if (last_pts == pts) {
    return;
  }
  last_pts = pts;
  sensor_msgs::msg::CameraInfo msg = camera_info_manager_.getCameraInfo();
  msg.header.stamp = get_time(pts);
  msg.header.frame_id = frame_id_;
  cinfo_pub_->publish(msg);
}

bool GSCamMulti::should_be_active() const {
  for (const auto &[k, v] : sinks) {
    if (v->active) return true;
  }
  return false;
}

void GSCamMulti::check_activation() {
  bool desired = should_be_active();
  // RCLCPP_INFO(get_logger(), "check cam activation %d %d", desired, active);
  if (desired != active) {
    if (desired) {
      start_stream();
    } else {
      stop_stream();
    }
  }
}

rcl_interfaces::msg::SetParametersResult GSCamMulti::parametersCallback(
    const std::vector<rclcpp::Parameter> &parameters) {
  rcl_interfaces::msg::SetParametersResult result;
  result.successful = true;
  for (const auto &parameter : parameters) {
    if (sinks.count(parameter.get_name()) &&
        parameter.get_type() == rclcpp::ParameterType::PARAMETER_INTEGER) {
      sinks.at(parameter.get_name())->set_activation(parameter.as_int());
    }
  }
  return result;
}

GSCamMulti::~GSCamMulti() {
  // stop_signal_ = true;

  gst_element_send_event(pipeline_, gst_event_new_eos());
  sleep(1);

  g_main_loop_quit(main_loop);
  pipeline_thread_.join();
  clean();
}

bool GSCamMulti::configure() {
  // Get gstreamer configuration
  // (either from environment variable or ROS param)
  bool gsconfig_rosparam_defined = false;
  char *gsconfig_env = NULL;

  const auto gsconfig_rosparam = declare_parameter("source", "");
  gsconfig_rosparam_defined = !gsconfig_rosparam.empty();
  gsconfig_env = getenv("SOURCE");

  if (!gsconfig_env && !gsconfig_rosparam_defined) {
    RCLCPP_FATAL(get_logger(),
                 "Problem getting SOURCE environment variable and "
                 "'source' rosparam is not set. This is needed to set up "
                 "a gstreamer pipeline.");
    return false;
  } else if (gsconfig_env && gsconfig_rosparam_defined) {
    RCLCPP_FATAL(get_logger(),
                 "Both SOURCE environment variable and 'source' "
                 "rosparam are set. "
                 "Please only define one.");
    return false;
  } else if (gsconfig_env) {
    gsconfig_ = gsconfig_env;
    RCLCPP_INFO_STREAM(get_logger(), "Using gstreamer config from env: \""
                                         << gsconfig_env << "\"");
  } else if (gsconfig_rosparam_defined) {
    gsconfig_ = gsconfig_rosparam;
    RCLCPP_INFO_STREAM(get_logger(), "Using gstreamer config from rosparam: \""
                                         << gsconfig_rosparam << "\"");
  }



  // Get additional GSCamMulti configuration
  sync_sink_ = declare_parameter("sync_sink", true);
  preroll_ = declare_parameter("preroll", false);
  use_gst_timestamps_ = declare_parameter("use_gst_timestamps", false);
  reopen_on_eof_ = declare_parameter("reopen_on_eof", false);

  // Get the camera parameters file
  camera_info_url_ = declare_parameter("camera_info_url", "");
  camera_name_ = declare_parameter("camera_name", "");

  camera_info_manager_.setCameraName(camera_name_);

  if (camera_info_manager_.validateURL(camera_info_url_)) {
    camera_info_manager_.loadCameraInfo(camera_info_url_);
    RCLCPP_INFO_STREAM(get_logger(),
                       "Loaded camera calibration from " << camera_info_url_);
  } else {
    RCLCPP_WARN_STREAM(
        get_logger(),
        "Camera info at: " << camera_info_url_
                           << " not found. Using an uncalibrated config.");
  }

  // Get TF Frame
  frame_id_ = declare_parameter("frame_id", "camera_optical_link");
  // if (frame_id_ == "camera_frame") {
  //   RCLCPP_WARN_STREAM(get_logger(), "No camera frame_id set, using frame \""
  //                                        << frame_id_ << "\".");
  // }

  use_sensor_data_qos_ = declare_parameter("use_sensor_data_qos", false);

  go_to_ready_when_paused = declare_parameter("go_to_ready_when_paused", true);

  return true;
}

bool GSCamMulti::stop_stream() {
  gst_element_set_state(pipeline_, go_to_ready_when_paused? GST_STATE_READY : GST_STATE_PAUSED);
  if (gst_element_get_state(pipeline_, NULL, NULL, -1) ==
      GST_STATE_CHANGE_FAILURE) {
    RCLCPP_FATAL(get_logger(),
                 "Failed to PAUSE stream, check your gstreamer configuration.");
    return false;
  } else {
    RCLCPP_INFO(get_logger(), "Stream is READY.");
  }
  active = false;
  return true;
}

bool GSCamMulti::init_stream() {
  if (!gst_is_initialized()) {
    // Initialize gstreamer pipeline
    RCLCPP_INFO_STREAM(get_logger(), "Initializing gstreamer...");
    gst_init(0, 0);
  }

  RCLCPP_INFO_STREAM(get_logger(),
                     "Gstreamer Version: " << gst_version_string());

  GError *error = 0;  // Assignment to zero is a gst requirement

  pipeline_ = gst_parse_launch(gsconfig_.c_str(), &error);
  if (pipeline_ == NULL) {
    RCLCPP_FATAL_STREAM(get_logger(), error->message);
    return false;
  }

  tee = gst_element_factory_make("tee", "tee");
  gst_bin_add_many(GST_BIN(pipeline_), tee, NULL);
  if (GST_IS_PIPELINE(pipeline_)) {
    GstPad *outpad = gst_bin_find_unlinked_pad(GST_BIN(pipeline_), GST_PAD_SRC);
    g_assert(outpad);

    GstElement *outelement = gst_pad_get_parent_element(outpad);
    g_assert(outelement);
    gst_object_unref(outpad);

    if (!gst_element_link(outelement, tee)) {
      RCLCPP_FATAL(get_logger(),
                   "GStreamer: cannot link outelement(\"%s\") -> tee\n",
                   gst_element_get_name(outelement));
      gst_object_unref(outelement);
      return false;
    }
    gst_object_unref(outelement);
  }

  GstClock *clock = gst_system_clock_obtain();
  GstClockTime ct = gst_clock_get_time(clock);
  gst_object_unref(clock);
  time_offset_ = now().nanoseconds() - GST_TIME_AS_NSECONDS(ct);
  RCLCPP_INFO(get_logger(), "Time offset: %.6f",
              rclcpp::Time(time_offset_).seconds());

  GstBus *bus = gst_element_get_bus(pipeline_);
  gst_bus_add_signal_watch(bus);
  g_signal_connect(G_OBJECT(bus), "message::error", (GCallback)error_cb, this);
  gst_object_unref(bus);

  init_sinks();

  return true;
}

void GSCamMulti::start_stream() {
  for (auto &[k, v] : sinks) {
    g_object_set(v->valve, "drop", false, nullptr);
  }

  if (preroll_) {
    RCLCPP_INFO(get_logger(), "Performing preroll...");
    // The PAUSE, PLAY, PAUSE, PLAY cycle is to ensure proper pre-roll
    // I am told this is needed and am erring on the side of caution.
    gst_element_set_state(pipeline_, GST_STATE_PLAYING);
    if (gst_element_get_state(pipeline_, NULL, NULL, -1) ==
        GST_STATE_CHANGE_FAILURE) {
      RCLCPP_ERROR(get_logger(), "Failed to PLAY during preroll.");
      return;
    } else {
      RCLCPP_INFO(get_logger(), "Stream is PLAYING in preroll.");
    }
    sleep(0);
    gst_element_set_state(pipeline_, GST_STATE_PAUSED);
    if (gst_element_get_state(pipeline_, NULL, NULL, -1) ==
        GST_STATE_CHANGE_FAILURE) {
      RCLCPP_ERROR(get_logger(), "Failed to PAUSE.");
      return;
    } else {
      RCLCPP_INFO(get_logger(), "Stream is PAUSED in preroll.");
    }
  }
  if (gst_element_set_state(pipeline_, GST_STATE_PLAYING) ==
      GST_STATE_CHANGE_FAILURE) {
    RCLCPP_ERROR(get_logger(), "Could not start stream!");
    return;
  }
  for (auto &[k, v] : sinks) {
    g_object_set(v->valve, "drop", true, nullptr);
  }
  RCLCPP_INFO(get_logger(), "Stream is PLAYING.");
  active = true;
}

#if 0
void GSCamMulti::publish_stream()
{
  RCLCPP_INFO_STREAM(get_logger(), "Publishing stream...");

  // Pre-roll camera if needed


  // Poll the data as fast a spossible
  while (!stop_signal_ && rclcpp::ok()) {
    GstSample * sample = gst_app_sink_pull_sample(GST_APP_SINK(raw_sink));
    if (!sample) {
      RCLCPP_ERROR(get_logger(), "Could not get gstreamer sample.");
      sleep(1);
      continue;
    }
    publish_raw(sample);
    gst_sample_unref(sample);
  }
}
#endif

void GSCamMulti::cleanup_stream() {
  // Clean up
  RCLCPP_INFO(get_logger(), "Stopping gstreamer pipeline...");
  if (pipeline_) {
    gst_element_set_state(pipeline_, GST_STATE_NULL);
    gst_object_unref(pipeline_);
    pipeline_ = NULL;
  }
}

void GSCamMulti::init() {
  if (!this->init_stream()) {
    RCLCPP_FATAL(get_logger(), "Failed to initialize GSCamMulti stream!");
    return;
  }
  const auto qos =
      use_sensor_data_qos_ ? rclcpp::SensorDataQoS() : rclcpp::QoS{1};
  cinfo_pub_ =
      create_publisher<sensor_msgs::msg::CameraInfo>("camera_info", qos);

  for (auto &[k, v] : sinks) {
    v->set_activation(get_parameter(v->name).as_int());
  }
}

void GSCamMulti::clean() {
  this->cleanup_stream();

  // RCLCPP_INFO(get_logger(), "GStreamer stream stopped!");

  // if (reopen_on_eof_) {
  //   RCLCPP_INFO(get_logger(), "Reopening stream...");
  // } else {
  //   RCLCPP_INFO(get_logger(), "Cleaning up stream and exiting...");
  // }
  rclcpp::shutdown();
}

void GSCamMulti::run() {
  // if (!this->configure()) {
  //   RCLCPP_FATAL(get_logger(), "Failed to configure GSCamMulti!");
  //   return;
  // }

  // if (!this->init_stream()) {
  //   RCLCPP_FATAL(get_logger(), "Failed to initialize GSCamMulti stream!");
  //   return;
  // }

  main_loop = g_main_loop_new(NULL, FALSE);

  // RCLCPP_INFO(get_logger(), "start g_main_loop_run");
  g_main_loop_run(main_loop);
  // RCLCPP_INFO(get_logger(), "ended g_main_loop_run");

  // this->cleanup_stream();

  // RCLCPP_INFO(get_logger(), "GStreamer stream stopped!");

  // if (reopen_on_eof_) {
  //   RCLCPP_INFO(get_logger(), "Reopening stream...");
  // } else {
  //   RCLCPP_INFO(get_logger(), "Cleaning up stream and exiting...");
  // }
  // rclcpp::shutdown();
}

#if 0
// Example callbacks for appsink
// TODO(someone): enable callback-based capture
void gst_eos_cb(GstAppSink *appsink, gpointer user_data) {}
GstFlowReturn gst_new_preroll_cb(GstAppSink *appsink, gpointer user_data) {
  return GST_FLOW_OK;
}
GstFlowReturn gst_new_asample_cb(GstAppSink *appsink, gpointer user_data) {
  return GST_FLOW_OK;
}

#endif

}  // namespace gscam

#include "rclcpp_components/register_node_macro.hpp"

RCLCPP_COMPONENTS_REGISTER_NODE(gscam::GSCamMulti)
