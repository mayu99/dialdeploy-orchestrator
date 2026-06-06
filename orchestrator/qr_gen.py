import qrcode
import io
import base64

def generate_qr_data_url(url: str, scale: int = 10) -> str:
    """Generates a base64 encoded data URI of a QR code pointing to the specified URL"""
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=scale,
        border=4,
    )
    qr.add_data(url)
    qr.make(fit=True)
    
    # Render with dark violet/indigo styling matching custom theme
    img = qr.make_image(fill_color="#4F46E5", back_color="white")
    
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    
    base64_data = base64.b64encode(buf.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{base64_data}"
