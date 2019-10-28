import math
from larrydata import s3 as s3
from io import BytesIO


def scale_image_to_size(image=None, bucket=None, key=None, uri=None, max_pixels=None, max_bytes=None):
    try:
        from PIL import Image
        if image:
            src_bytes = _image_bytes(image)
        else:
            src_bytes = s3.get_object_size(bucket, key, uri)
            image = s3.read_pillow_image(bucket, key, uri)
        x, y = image.size
        src_pixels = x * y
        bytes_scalar = math.sqrt(max_bytes/src_bytes) if max_bytes else 1
        pixels_scalar = math.sqrt(max_pixels/src_pixels) if max_pixels else 1
        scalar = min(bytes_scalar, pixels_scalar)
        if scalar >= 1:
            return image, None
        else:
            new_x = int(scalar * x)
            new_y = int(scalar * y)
            new_image, scalar = image.resize((new_x, new_y), Image.BICUBIC), scalar
            if max_bytes and _image_bytes(new_image) > max_bytes:
                return scale_image_to_size(image=image, max_pixels=max_pixels, max_bytes=int(max_bytes*0.95))
            else:
                return new_image, scalar
    except ImportError as e:
        # Simply raise the ImportError to let the user know this requires Pillow to function
        raise e


def _image_bytes(image):
    buff = BytesIO()
    image.save(buff, 'PNG' if image.format is None else image.format)
    return len(buff)
