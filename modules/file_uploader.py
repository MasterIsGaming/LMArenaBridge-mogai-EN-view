import httpx
import logging
import base64
import ssl
from typing import Tuple, Optional, Any

logger = logging.getLogger(__name__)

def _get_value_from_json_path(json_data: dict, path: str) -> Optional[str]:
    """Retrieves a value from JSON data based on a dot-separated path, supporting array indexing."""
    keys = path.split('.')
    current_data = json_data
    for key in keys:
        if isinstance(current_data, dict):
            current_data = current_data.get(key)
        elif isinstance(current_data, list) and key.isdigit():
            try:
                current_data = current_data[int(key)]
            except IndexError:
                current_data = None
        else:
            current_data = None

        if current_data is None:
            return None
    return str(current_data) if current_data is not None else None

async def upload_to_file_bed(
        file_name: str,
        file_data: str,
        endpoint: dict
) -> Tuple[Optional[str], Optional[str]]:
    """
    Uploads a base64 encoded file to a specified file bed endpoint (generic version).
    """
    upload_url = endpoint.get("url")
    endpoint_name = endpoint.get("name", "Unknown Endpoint")

    if not upload_url:
        return None, f"Image bed '{endpoint_name}' has no 'url' configured."

    # Read API specific requirements from the configuration
    file_field_name = endpoint.get("form_file_field", "file")
    extra_data_fields = endpoint.get("form_data_fields", {})
    response_type = endpoint.get("response_type", "json")  # "json" or "text"
    json_url_key = endpoint.get("json_url_key", "url")  # e.g., "data.url" or "image"
    api_key = endpoint.get("api_key")
    api_key_field = endpoint.get("api_key_field", "key")

    if ',' in file_data:
        _, file_data = file_data.split(',', 1)

    try:
        decoded_file_data = base64.b64decode(file_data)

        files_payload = {file_field_name: (file_name, decoded_file_data)}

        # Add api_key to extra data (if present)
        if api_key:
            extra_data_fields[api_key_field] = api_key

        # Create a more lenient SSL context to resolve "sslv3 alert handshake failure" issues
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        ssl_context.set_ciphers('DEFAULT@SECLEVEL=1')

        # Allow and handle redirects to be compatible with endpoints like filepush.co
        async with httpx.AsyncClient(timeout=60.0, verify=ssl_context, follow_redirects=True) as client:
            response = await client.post(upload_url, data=extra_data_fields, files=files_payload)
            # For non-200 status codes, also try to process, as some image beds return non-2xx status codes on success.
            # response.raise_for_status() # Temporarily commented out to handle more cases

            final_url = None
            if response.status_code == 200:
                # Prioritize checking the 'Location' field in the response headers, applicable for some redirect scenarios
                if 'Location' in response.headers and response.headers['Location'].startswith("http"):
                    final_url = response.headers['Location']
                elif response_type == 'text':
                    url = response.text.strip()
                    # Compatible with "wget <url>" format returned by bashupload.com
                    if "wget" in url:
                        import re
                        match = re.search(r'https?://\S+', url)
                        if match:
                            final_url = match.group(0)
                    elif url.startswith("http"):
                        final_url = url
                else:  # Defaults to json
                    try:
                        result = response.json()
                        final_url = _get_value_from_json_path(result, json_url_key)
                    except Exception:
                        # If JSON parsing fails (e.g., temp.sh), fall back to text mode
                        url = response.text.strip()
                        if url.startswith("http"):
                            final_url = url

            if final_url:
                logger.info(f"✅ Successfully uploaded '{file_name}' to '{endpoint_name}', URL: {final_url}")
                return final_url, None
            else:
                # Improved error message, including status code
                error_msg = f"Image bed returned an unexpected response (HTTP {response.status_code}): {response.text[:200]}"
                logger.error(f"❌ Upload to '{endpoint_name}' failed: {error_msg}")
                return None, error_msg

    except httpx.HTTPStatusError as e:
        error_details = f"HTTP Error {e.response.status_code} - {e.response.text[:200]}"
        return None, error_details
    except httpx.RequestError as e:
        # Improved logging, including exception type
        error_details = f"Connection error ({type(e).__name__}): {e}. Please check network and URL configuration for '{endpoint_name}'."
        return None, error_details
    except Exception as e:
        # Improved logging, including exception type
        error_details = f"Unknown upload error ({type(e).__name__}): {e}"
        return None, error_details
