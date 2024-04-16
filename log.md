# 進度
- 3/23 臉部偵測(包含bounding box)、人臉搜尋。
- 3/27 lambda建立，上傳至s3時觸發，搜尋後儲存結果
- 3/29 臉部偵測列出更多臉部細節
- 4/2  把偵測和搜尋結合起來
    - video_face_detection_search.py中GetFaceSearchCollectionResults()函數底下search_results中有重複的Timestamp，但是GetFinalResult()中沒有解決
    - 假如偵測不到(Name:Unknow)則FaceMatch底下的similarity會繼承前一比配對到的人臉資料所以統一改為0.00%
    - GetFinalResult()底下的search_results_dict只會紀錄同樣的Timestamp中最後一筆出現的Timestamp並以該Timestamp當作Key另存為新的dict，導致輸出結果錯誤
- 4/9 多