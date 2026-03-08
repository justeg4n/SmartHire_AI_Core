import json

def calculate_interview_metrics(emotion_timeline):
    """
    Thuật toán xử lý hàng trăm bản ghi cảm xúc để đưa ra báo cáo tổng quan.
    """
    total_frames = len(emotion_timeline)
    if total_frames == 0:
        return {"error": "Không có dữ liệu cảm xúc để phân tích."}

    # Các biến đếm
    calm_frames = 0
    stress_frames = 0
    good_attention_frames = 0
    peak_stress_moments = []

    # Ngưỡng (Thresholds) cấu hình
    STRESS_EMOTIONS = ["FEAR", "CONFUSED", "SAD"]
    STRESS_THRESHOLD = 30.0 # Nếu tự tin > 30% cho các cảm xúc tiêu cực -> Đánh dấu là căng thẳng
    YAW_TOLERANCE = 25.0    # Góc quay đầu trái/phải tối đa để tính là đang nhìn màn hình
    PITCH_TOLERANCE = 20.0  # Góc cúi/ngửa đầu tối đa

    for frame in emotion_timeline:
        timestamp = frame['timestamp']
        top_emotion = frame['top_emotions'][0] # Cảm xúc rõ nhất trong khung hình này
        
        # 1. Tính toán % Bình tĩnh
        if top_emotion['type'] == "CALM" or top_emotion['type'] == "HAPPY":
            calm_frames += 1
            
        # 2. Theo dõi các khoảnh khắc Căng thẳng (Stress)
        is_stressed = False
        for emotion in frame['top_emotions']:
            if emotion['type'] in STRESS_EMOTIONS and emotion['confidence'] >= STRESS_THRESHOLD:
                is_stressed = True
                break
                
        if is_stressed:
            stress_frames += 1
            # Ghi lại mốc thời gian nếu căng thẳng rất cao (Peak Stress > 60%)
            if emotion['confidence'] >= 60.0:
                peak_stress_moments.append({
                    "time": timestamp,
                    "trigger": emotion['type'],
                    "intensity": round(emotion['confidence'], 2)
                })

        # 3. Đánh giá sự tập trung (Eye Contact / Pose)
        pose = frame['head_pose']
        if abs(pose['yaw']) <= YAW_TOLERANCE and abs(pose['pitch']) <= PITCH_TOLERANCE:
            good_attention_frames += 1

    # Tính toán chỉ số % cuối cùng
    percent_calm = round((calm_frames / total_frames) * 100, 1)
    attention_score = round((good_attention_frames / total_frames) * 100, 1)
    overall_stress_level = round((stress_frames / total_frames) * 100, 1)

    return {
        "metrics": {
            "percent_time_calm": percent_calm,
            "average_attention_score": attention_score,
            "overall_stress_level": overall_stress_level
        },
        "insights": {
            "peak_stress_moments": peak_stress_moments, # Trả về mốc thời gian để Frontend làm tính năng Replay
            "recommendation": "Candidate maintained good composure." if percent_calm >= 60 else "Candidate showed significant signs of stress or confusion."
        }
    }

# --- TEST THUẬT TOÁN ---
if __name__ == "__main__":
    # Giả lập dữ liệu Backend (Step Functions) truyền vào sau khi kết thúc phỏng vấn
    mock_dynamodb_data = [
        {"timestamp": "00:05", "top_emotions": [{"type": "CALM", "confidence": 92.0}], "head_pose": {"yaw": 5.1, "pitch": 2.0}},
        {"timestamp": "00:10", "top_emotions": [{"type": "CALM", "confidence": 88.0}], "head_pose": {"yaw": 8.0, "pitch": 3.5}},
        {"timestamp": "00:15", "top_emotions": [{"type": "CONFUSED", "confidence": 65.5}, {"type": "FEAR", "confidence": 20.0}], "head_pose": {"yaw": -15.2, "pitch": 5.0}},
        {"timestamp": "00:20", "top_emotions": [{"type": "FEAR", "confidence": 75.0}], "head_pose": {"yaw": 35.0, "pitch": 10.0}}, # Chỗ này ứng viên ngoảnh mặt đi chỗ khác do hoảng
        {"timestamp": "00:25", "top_emotions": [{"type": "CALM", "confidence": 80.0}], "head_pose": {"yaw": 2.0, "pitch": 1.0}}
    ]

    report_data = calculate_interview_metrics(mock_dynamodb_data)
    print("\n--- KẾT QUẢ TỔNG HỢP CẢM XÚC CHO DASHBOARD ---")
    print(json.dumps(report_data, indent=2))