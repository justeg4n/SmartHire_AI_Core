import boto3

def synthesize_ai_voice(ai_text_response, output_filename="ai_voice.mp3"):
    """
    Chuyển đổi văn bản thành giọng nói Neural có độ trễ thấp và ngắt nghỉ tự nhiên.
    """
    polly = boto3.client('polly')
    
    # Bọc text trong thẻ SSML để thêm khoảng nghỉ (pause) tự nhiên
    # Ví dụ: Thêm thẻ <break> sau dấu chấm câu.
    ssml_text = f"<speak><prosody rate='medium'>{ai_text_response}</prosody></speak>"
    
    print("Calling Amazon Polly (Neural TTS)...")
    try:
        response = polly.synthesize_speech(
            Text=ssml_text,
            OutputFormat='mp3',
            TextType='ssml',
            VoiceId='Matthew',
            Engine='neural'
        )
        
        # Lưu luồng audio thành file mp3 để test
        if "AudioStream" in response:
            with open(output_filename, "wb") as file:
                file.write(response["AudioStream"].read())
            print(f"Thành công! File âm thanh đã được lưu tại: {output_filename}")
            
    except Exception as e:
        print(f"Lỗi khi gọi Amazon Polly: {e}")

# --- TEST ---
if __name__ == "__main__":
    sample_text = "That's an interesting approach to the for loop. Could you explain why you chose that over LINQ?"
    synthesize_ai_voice(sample_text)