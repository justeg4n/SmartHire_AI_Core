import boto3
import json

def analyze_candidate_frame(image_path):
    # 1. Initialize the Rekognition Client
    rekognition = boto3.client('rekognition')
    
    # 2. Read the image file
    print(f"Loading image: {image_path}...")
    try:
        with open(image_path, 'rb') as image_file:
            image_bytes = image_file.read()
    except FileNotFoundError:
        return {"error": f"Could not find the file {image_path}."}

    # 3. Call Amazon Rekognition
    print("Sending frame to Amazon Rekognition...")
    response = rekognition.detect_faces(
        Image={'Bytes': image_bytes},
        Attributes=['ALL']
    )
    
    if not response['FaceDetails']:
        return {"error": "No face detected in the image."}
    
    candidate_face = response['FaceDetails'][0]
    
    # 4. Extract ALL 8 Emotions and sort them by confidence
    all_emotions = candidate_face['Emotions']
    sorted_emotions = sorted(all_emotions, key=lambda x: x['Confidence'], reverse=True)
    
    # Dynamically format the entire list using a list comprehension
    formatted_emotions = [
        {"type": emotion['Type'], "confidence": round(emotion['Confidence'], 2)}
        for emotion in sorted_emotions
    ]
    
    # 5. Extract Pose (Eye contact proxy)
    pose = candidate_face['Pose']
    
    # 6. Final JSON Output Structure
    result = {
        "status": "success",
        "emotions_profile": formatted_emotions, # This now contains all 8 emotions
        "head_pose": {
            "yaw": round(pose['Yaw'], 2),
            "pitch": round(pose['Pitch'], 2)
        }
    }
    
    return result

# --- TEST THE FUNCTION ---
if __name__ == "__main__":
    target_image = 'test_face.jpg'
    
    analysis_result = analyze_candidate_frame(target_image)
    
    print("\n--- FULL AI VISION OUTPUT ---")
    print(json.dumps(analysis_result, indent=2))