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

#ifndef GSCamMulti__GSCamMulti_HPP_
#define GSCamMulti__GSCamMulti_HPP_

#include <stdexcept>
#include <string>

extern "C" {
#include "gst/app/gstappsink.h"
#include "gst/gst.h"
}

#include "camera_info_manager/camera_info_manager.hpp"
#include "ffmpeg_image_transport_msgs/msg/ffmpeg_packet.hpp"
#include "image_transport/image_transport.hpp"
#include "rcl_interfaces/msg/parameter_event.hpp"
#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/camera_info.hpp"
#include "sensor_msgs/msg/compressed_image.hpp"
#include "sensor_msgs/msg/image.hpp"
#include "sensor_msgs/srv/set_camera_info.hpp"

namespace gscam {

enum class ActiveMode : int { OFF = 0, ON = 1, ON_DEMAND = 2 };

class GSCamMulti;

struct Sink {
  explicit Sink(GSCamMulti *cam, const std::string &name)
      : cam(cam),
        activation(ActiveMode::OFF),
        active(false),
        name(name),
        eos(false){};
  ~Sink(){};
  bool start();
  bool stop();
  void set_activation(int v);
  void check_activation();
  virtual bool should_be_active() const;
  bool init(GstElement *bin);

  GSCamMulti *cam;
  ActiveMode activation;
  bool active;
  GstElement *valve;
  std::string name;
  GstElement *sink;
  bool eos;
};

struct ROSSink : Sink {
  explicit ROSSink(GSCamMulti *cam, const std::string &name)
      : Sink(cam, name), subscribers(0){};
  ~ROSSink(){};
  virtual bool should_be_active() const override;
  virtual bool init(GstElement *bin, const GstCaps *caps,
                    const std::string &topic, const std::string &frame_id,
                    const rclcpp::QoS &qos);
  virtual void publish(GstSample *sample) = 0;

  int subscribers;
};

struct RawROSSink : ROSSink {
  explicit RawROSSink(GSCamMulti *cam, const std::string &name)
      : ROSSink(cam, name), pub(nullptr){};

  bool init(GstElement *bin, const GstCaps *caps, const std::string &topic,
            const std::string &frame_id, const rclcpp::QoS &qos) override;
  void publish(GstSample *sample) override;

  rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr pub;
  sensor_msgs::msg::Image msg;
  // GstCaps *raw_caps;
};

struct JPEGROSSink : ROSSink {
  explicit JPEGROSSink(GSCamMulti *cam, const std::string &name)
      : ROSSink(cam, name), pub(nullptr), jpegenc(nullptr){};

  bool init(GstElement *bin, const GstCaps *caps, const std::string &topic,
            const std::string &frame_id, const rclcpp::QoS &qos) override;
  void publish(GstSample *sample) override;

  rclcpp::Publisher<sensor_msgs::msg::CompressedImage>::SharedPtr pub;
  sensor_msgs::msg::CompressedImage msg;
  GstElement *jpegenc;
};

struct FFMPEGROSSink : ROSSink {
  explicit FFMPEGROSSink(GSCamMulti *cam, const std::string &name,
                         const std::string &encoding)
      : ROSSink(cam, name),
        pub(nullptr),
        msg(),
        h264enc(nullptr),
        h264parse(nullptr),
        videoconvert(nullptr),
        pts(0) {
    msg.encoding = encoding;
  };

  bool init(GstElement *bin, const GstCaps *caps, const std::string &topic,
            const std::string &frame_id, const rclcpp::QoS &qos) override;
  void publish(GstSample *sample) override;

  rclcpp::Publisher<ffmpeg_image_transport_msgs::msg::FFMPEGPacket>::SharedPtr
      pub;
  ffmpeg_image_transport_msgs::msg::FFMPEGPacket msg;
  GstElement *h264enc;
  GstElement *h264parse;
  GstElement *videoconvert;
  unsigned long pts;
};

class GSCamMulti : public rclcpp::Node {
 public:
  explicit GSCamMulti(const rclcpp::NodeOptions &options);
  ~GSCamMulti();

 public:
  // void set_activation(int v);
  // void check_activation();
  // bool should_be_active() const;

  rcl_interfaces::msg::SetParametersResult parametersCallback(
      const std::vector<rclcpp::Parameter> &parameters);

  bool configure();
  bool init_stream();
  // void publish_stream();
  void cleanup_stream();
  void start_stream();
  bool stop_stream();
  void run();
  void init();
  void clean();
  bool should_be_active() const;
  void check_activation();
  void publish_info(uint64_t pts);
  rclcpp::Time get_time(uint64_t pts) const;

  void init_sinks();
  void add_ros_sink(const std::string &name, std::string config,
                    std::string caps, std::string topic);
  void add_sink(const std::string &name, const std::string &config, bool eos);
  std::map<std::string, std::shared_ptr<Sink>> sinks;

  GMainLoop *main_loop;

  bool active;

  OnSetParametersCallbackHandle::SharedPtr callback_handle_;

  // General gstreamer configuration
  std::string gsconfig_;

  // Gstreamer structures
  GstElement *pipeline_;
  // GstElement * outelement;
  GstElement *tee;

  // Appsink configuration
  bool sync_sink_;
  bool preroll_;
  bool reopen_on_eof_;
  bool use_gst_timestamps_;

  // Camera publisher configuration
  std::string frame_id_;
  std::string camera_name_;
  std::string camera_info_url_;
  bool use_sensor_data_qos_;
  bool go_to_ready_when_paused;

  // ROS Inteface
  // Calibration between ros::Time and gst timestamps
  uint64_t time_offset_;
  camera_info_manager::CameraInfoManager camera_info_manager_;
  rclcpp::Publisher<sensor_msgs::msg::CameraInfo>::SharedPtr cinfo_pub_;

  uint64_t last_pts;

  // Poll gstreamer on a separate thread
  std::thread pipeline_thread_;
  std::atomic<bool> stop_signal_;
};

}  // namespace gscam

#endif  // GSCamMulti__GSCamMulti_HPP_
