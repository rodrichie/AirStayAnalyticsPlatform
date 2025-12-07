"""
Image Processing for Property Photos
- Classify images (bedroom, kitchen, bathroom, exterior)
- Calculate quality scores
- Generate thumbnails
- Extract dominant colors
"""
import os
import logging
from typing import Dict, List, Tuple
from io import BytesIO
import numpy as np

try:
    from PIL import Image
    import torch
    import torchvision.transforms as transforms
    from torchvision import models
    import cv2
    HAS_VISION = True
except ImportError:
    HAS_VISION = False
    logging.warning("Vision libraries not available")

from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PropertyImageProcessor:
    """Process property images for quality and classification"""
    
    def __init__(self, minio_endpoint="minio:9000"):
        """Initialize image processor"""
        self.minio_client = Minio(
            minio_endpoint,
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            secure=False
        )
        
        if HAS_VISION:
            # Load pre-trained ResNet for feature extraction
            self.model = models.resnet50(pretrained=True)
            self.model.eval()
            
            # Image preprocessing
            self.transform = transforms.Compose([
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406],
                    std=[0.229, 0.224, 0.225]
                )
            ])
            
            logger.info("✅ Image processor initialized with ResNet50")
        else:
            self.model = None
            logger.warning("⚠️ Image processing in mock mode")
    
    def download_image(self, bucket: str, object_key: str) -> Image.Image:
        """Download image from MinIO"""
        try:
            response = self.minio_client.get_object(bucket, object_key)
            image_data = response.read()
            response.close()
            response.release_conn()
            
            image = Image.open(BytesIO(image_data))
            return image.convert('RGB')
            
        except Exception as e:
            logger.error(f"Error downloading image {object_key}: {e}")
            return None
    
    def upload_image(self, bucket: str, object_key: str, 
                     image: Image.Image) -> bool:
        """Upload processed image to MinIO"""
        try:
            buffer = BytesIO()
            image.save(buffer, format='JPEG', quality=85)
            buffer.seek(0)
            
            self.minio_client.put_object(
                bucket,
                object_key,
                buffer,
                length=buffer.getbuffer().nbytes,
                content_type='image/jpeg'
            )
            
            logger.info(f"✅ Uploaded {object_key}")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading image {object_key}: {e}")
            return False
    
    def calculate_quality_score(self, image: Image.Image) -> float:
        """
        Calculate image quality score based on:
        - Resolution
        - Sharpness
        - Brightness
        - Contrast
        """
        if not HAS_VISION:
            return 0.75  # Mock score
        
        try:
            # Convert to numpy array
            img_array = np.array(image)
            
            # 1. Resolution score (higher is better, capped at 1920x1080)
            width, height = image.size
            resolution_score = min((width * height) / (1920 * 1080), 1.0)
            
            # 2. Sharpness score (Laplacian variance)
            gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)
            laplacian_var = cv2.Laplacian(gray, cv2.CV_64F).var()
            sharpness_score = min(laplacian_var / 500, 1.0)
            
            # 3. Brightness score (optimal around 128)
            brightness = np.mean(img_array)
            brightness_score = 1 - abs(brightness - 128) / 128
            
            # 4. Contrast score (standard deviation)
            contrast = np.std(img_array)
            contrast_score = min(contrast / 75, 1.0)
            
            # Weighted average
            quality_score = (
                resolution_score * 0.3 +
                sharpness_score * 0.4 +
                brightness_score * 0.15 +
                contrast_score * 0.15
            )
            
            return round(quality_score, 3)
            
        except Exception as e:
            logger.error(f"Error calculating quality: {e}")
            return 0.5
    
    def classify_room_type(self, image: Image.Image) -> Dict:
        """
        Classify image into room types:
        - bedroom, kitchen, bathroom, living_room, exterior, other
        """
        if not HAS_VISION or self.model is None:
            # Mock classification
            return {
                'room_type': 'bedroom',
                'confidence': 0.85
            }
        
        try:
            # Preprocess image
            img_tensor = self.transform(image).unsqueeze(0)
            
            # Get features from ResNet
            with torch.no_grad():
                features = self.model(img_tensor)
            
            # Simple heuristic classification based on features
            # In production, you'd fine-tune a model on room type dataset
            
            # For now, use ImageNet classes as proxy
            probs = torch.nn.functional.softmax(features, dim=1)
            top_prob, top_class = torch.topk(probs, 1)
            
            # Map ImageNet classes to room types (simplified)
            class_id = top_class.item()
            
            # This is a simplified mapping - in production, train a proper classifier
            if 500 <= class_id <= 550:
                room_type = 'bedroom'
            elif 550 <= class_id <= 600:
                room_type = 'kitchen'
            elif 600 <= class_id <= 650:
                room_type = 'bathroom'
            elif 650 <= class_id <= 700:
                room_type = 'living_room'
            elif 700 <= class_id <= 800:
                room_type = 'exterior'
            else:
                room_type = 'other'
            
            return {
                'room_type': room_type,
                'confidence': float(top_prob.item()),
                'class_id': class_id
            }
            
        except Exception as e:
            logger.error(f"Error classifying image: {e}")
            return {
                'room_type': 'other',
                'confidence': 0.0
            }
    
    def extract_dominant_colors(self, image: Image.Image, 
                                n_colors: int = 5) -> List[Tuple[int, int, int]]:
        """Extract dominant colors using k-means clustering"""
        try:
            # Resize for faster processing
            img_small = image.resize((150, 150))
            img_array = np.array(img_small)
            
            # Reshape to list of pixels
            pixels = img_array.reshape(-1, 3)
            
            # Use k-means to find dominant colors
            from sklearn.cluster import KMeans
            
            kmeans = KMeans(n_clusters=n_colors, random_state=42, n_init=10)
            kmeans.fit(pixels)
            
            # Get cluster centers (dominant colors)
            colors = kmeans.cluster_centers_.astype(int)
            
            # Sort by frequency
            labels = kmeans.labels_
            counts = np.bincount(labels)
            sorted_indices = np.argsort(-counts)
            
            dominant_colors = [tuple(colors[i]) for i in sorted_indices]
            
            return dominant_colors
            
        except Exception as e:
            logger.error(f"Error extracting colors: {e}")
            return [(128, 128, 128)]
    
    def generate_thumbnail(self, image: Image.Image, 
                          size: Tuple[int, int] = (300, 200)) -> Image.Image:
        """Generate thumbnail with aspect ratio preserved"""
        try:
            # Calculate aspect ratio
            img_ratio = image.width / image.height
            thumb_ratio = size[0] / size[1]
            
            if img_ratio > thumb_ratio:
                # Image is wider
                new_width = size[0]
                new_height = int(size[0] / img_ratio)
            else:
                # Image is taller
                new_height = size[1]
                new_width = int(size[1] * img_ratio)
            
            thumbnail = image.resize((new_width, new_height), Image.Resampling.LANCZOS)
            
            # Create canvas and paste thumbnail centered
            canvas = Image.new('RGB', size, (255, 255, 255))
            offset = ((size[0] - new_width) // 2, (size[1] - new_height) // 2)
            canvas.paste(thumbnail, offset)
            
            return canvas
            
        except Exception as e:
            logger.error(f"Error generating thumbnail: {e}")
            return image
    
    def process_property_images(self, property_id: int, 
                                image_urls: List[str]) -> Dict:
        """
        Process all images for a property
        
        Returns:
            Dictionary with processed image metadata
        """
        results = {
            'property_id': property_id,
            'total_images': len(image_urls),
            'processed_images': [],
            'avg_quality_score': 0.0,
            'room_types': {},
            'dominant_colors': [],
            'has_exterior': False
        }
        
        quality_scores = []
        
        for idx, url in enumerate(image_urls):
            try:
                # Extract object key from URL
                object_key = url.split('/')[-1]
                
                # Download image
                image = self.download_image('property-images', object_key)
                if image is None:
                    continue
                
                # Calculate quality score
                quality = self.calculate_quality_score(image)
                quality_scores.append(quality)
                
                # Classify room type
                classification = self.classify_room_type(image)
                room_type = classification['room_type']
                
                # Update room type counts
                results['room_types'][room_type] = results['room_types'].get(room_type, 0) + 1
                
                if room_type == 'exterior':
                    results['has_exterior'] = True
                
                # Extract dominant colors (only for first image)
                if idx == 0:
                    results['dominant_colors'] = self.extract_dominant_colors(image)
                
                # Generate and upload thumbnail
                thumbnail = self.generate_thumbnail(image)
                thumb_key = f"thumbnails/{property_id}/{object_key}"
                self.upload_image('processed-images', thumb_key, thumbnail)
                
                # Store processed image info
                results['processed_images'].append({
                    'original_url': url,
                    'thumbnail_url': f"http://minio:9000/processed-images/{thumb_key}",
                    'quality_score': quality,
                    'room_type': room_type,
                    'confidence': classification['confidence']
                })
                
            except Exception as e:
                logger.error(f"Error processing image {url}: {e}")
                continue
        
        # Calculate average quality
        if quality_scores:
            results['avg_quality_score'] = round(np.mean(quality_scores), 3)
        
        logger.info(f"✅ Processed {len(results['processed_images'])} images for property {property_id}")
        
        return results


# Example usage
if __name__ == "__main__":
    processor = PropertyImageProcessor()
    
    # Test with sample image URLs
    sample_urls = [
        "property-images/sample1.jpg",
        "property-images/sample2.jpg"
    ]
    
    results = processor.process_property_images(
        property_id=1,
        image_urls=sample_urls
    )
    
    print(f"📊 Processing Results:")
    print(f"   - Images processed: {len(results['processed_images'])}")
    print(f"   - Avg quality: {results['avg_quality_score']}")
    print(f"   - Room types: {results['room_types']}")
    print(f"   - Has exterior: {results['has_exterior']}")