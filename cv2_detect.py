import cv2
import numpy as np
import mediapipe as mp
from mediapipe.tasks import python
from mediapipe.tasks.python import vision

# 設定方法
BaseOptions = mp.tasks.BaseOptions
FaceDetector = mp.tasks.vision.FaceDetector
FaceDetectorOptions = mp.tasks.vision.FaceDetectorOptions
VisionRunningMode = mp.tasks.vision.RunningMode

# 人臉偵測設定
options = FaceDetectorOptions(
    base_options=BaseOptions(
        model_asset_path='./blaze_face_short_range.tflite'),
    running_mode=VisionRunningMode.VIDEO)

with FaceDetector.create_from_options(options) as detector:
    cap = cv2.VideoCapture('./video_detect05.mp4')
    fps=cap.get(cv2.CAP_PROP_FPS)
    img_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    img_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fourcc=cv2.VideoWriter_fourcc(*'mp4v')
    video =cv2.VideoWriter('./output_mediapipe.mp4',fourcc,fps,(img_width,img_height))

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        timestamp = int(cap.get(cv2.CAP_PROP_POS_MSEC))
        mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=frame)
        face_detector_result = detector.detect_for_video(mp_image, timestamp)
        for detection in face_detector_result.detections:
            boundingBox=detection.bounding_box
            x = boundingBox.origin_x                
            y = boundingBox.origin_y                
            width = boundingBox.width               
            height = boundingBox.height  
            cv2.rectangle(frame,(x,y),(x+width,y+height),(0,0,255),2)

        video.write(frame)

    cap.release()
    video.release()
