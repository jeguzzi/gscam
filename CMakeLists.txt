cmake_minimum_required(VERSION 3.5)

project(gscam)

# System Dependencies
find_package(PkgConfig)

pkg_check_modules(GSTREAMER REQUIRED gstreamer-1.0)
pkg_check_modules(GST_APP REQUIRED gstreamer-app-1.0)

find_package(ament_cmake REQUIRED)
find_package(class_loader REQUIRED)
find_package(image_transport REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_components REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(camera_calibration_parsers REQUIRED)
find_package(camera_info_manager REQUIRED)
find_package(ffmpeg_image_transport_msgs REQUIRED)


link_directories("/opt/homebrew/Cellar/gstreamer/1.22.6/lib" "/opt/homebrew/Cellar/glib/2.78.0/lib" "/opt/homebrew/Cellar/gettext/0.22.3/lib")

include_directories(
  include
  ${GLIB_INCLUDE_DIRS}
  ${GST_APP_INCLUDE_DIRS})

add_library(gscam SHARED src/gscam.cpp)
target_link_libraries(gscam
  ${ament_LIBRARIES}
  ${GSTREAMER_LIBRARIES}
  ${GST_APP_LIBRARIES})
ament_target_dependencies(gscam
  "class_loader"
  "image_transport"
  "rclcpp"
  "rclcpp_components"
  "sensor_msgs"
  "camera_calibration_parsers"
  "camera_info_manager")

rclcpp_components_register_node(gscam
  PLUGIN "gscam::GSCam"
  EXECUTABLE gscam_node
)

# message("GSTREAMER_LIBRARIES ${GSTREAMER_LIBRARIES}")

add_library(gscam_multi SHARED src/gscam_multi.cpp)
target_link_libraries(gscam_multi
  ${ament_LIBRARIES}
  ${GSTREAMER_LIBRARIES}
  ${GST_APP_LIBRARIES})
ament_target_dependencies(gscam_multi
  "class_loader"
  "image_transport"
  "rclcpp"
  "rclcpp_components"
  "sensor_msgs"
  "ffmpeg_image_transport_msgs"
  "camera_calibration_parsers"
  "camera_info_manager")

rclcpp_components_register_node(gscam_multi
  PLUGIN "gscam::GSCamMulti"
  EXECUTABLE gscam_multi_node
)


set(node_plugins "${node_plugins}gscam::GSCam;$<TARGET_FILE:gscam>\n")

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()

  find_package(ament_cmake_gtest)
  ament_add_gtest(
    smoke_test
    test/smoke_test.cpp
    ENV GSCAM_CONFIG="videotestsrc pattern=snow ! capsfilter caps=video/x-raw,width=800,height=600 ! videoconvert"
  )
  if(TARGET smoke_test)
    target_link_libraries(smoke_test gscam)
  endif()
endif()

ament_export_dependencies(class_loader)

ament_export_include_directories(include)

ament_export_targets(export_gscam export_gscam_multi)

ament_export_libraries(gscam gscam_multi)

ament_package()

# Install directives

install(TARGETS gscam
  EXPORT export_gscam
  DESTINATION lib
)

install(TARGETS gscam_multi
  EXPORT export_gscam_multi
  DESTINATION lib
)

install(
  DIRECTORY "examples"
  DESTINATION "share/${PROJECT_NAME}"
)
