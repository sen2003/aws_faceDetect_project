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
    cap = cv2.VideoCapture('./input.mp4')
    while True:
        ret, frame = cap.read()
