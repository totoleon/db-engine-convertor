import logging
import functools
import time
import os



def retry_on_quota_exceeded(max_attempts=30, initial_delay=1.0, backoff_factor=2):
    """A decorator for retrying a function call with exponential backoff on quota errors.

    Retries if the error message contains "Quota exceeded" or "Content has no parts".

    Args:
        max_attempts: Maximum number of attempts.
        initial_delay: Initial delay between retries in seconds.
        backoff_factor: Factor by which the delay increases after each retry.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            delay = initial_delay
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if (
                        "429" in str(e)
                        or "529" in str(e)
                        or "503" in str(e)
                        or "Quota exceeded" in str(e)
                        or "Content has no parts" in str(e)
                        or "Cannot get the response text" in str(e)
                    ) and attempts < max_attempts:
                        logging.warning(
                            f"Retriable error: {e}, retrying {attempts + 1}/{max_attempts} in {delay:.1f}s"
                        )
                        time.sleep(delay)
                        delay *= backoff_factor
                        attempts += 1
                    else:
                        logging.error(f"Final attempt failed or non-quota error: {e}")
                        raise
        return wrapper
    return decorator


@retry_on_quota_exceeded()
def gemini_inference(prompt, temperature: float = 0.3, enforce_json=True):
    from google import genai
    from google.genai import types
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or ""
    VERTEX_GCP_PROJECT = "hailongli-senseai"
    api_key = GEMINI_API_KEY
    if api_key:
        client = genai.Client(
            api_key=api_key,
        )
    else:
        client = genai.Client(
            vertexai=True,
            project=VERTEX_GCP_PROJECT,
            location="global",
        )
    model = "gemini-2.5-pro"
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(text=prompt),
            ],
        ),
    ]
    if enforce_json:
        mime_type = "application/json"
    else:
        mime_type = "text/plain"
    generate_content_config = types.GenerateContentConfig(
        response_mime_type=mime_type,
        temperature=temperature,
        safety_settings = [types.SafetySetting(
        category="HARM_CATEGORY_HATE_SPEECH",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_DANGEROUS_CONTENT",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_SEXUALLY_EXPLICIT",
        threshold="OFF"
        ),types.SafetySetting(
        category="HARM_CATEGORY_HARASSMENT",
        threshold="OFF"
        )],
    )
    response = client.models.generate_content(
        model=model,
        contents=contents,
        config=generate_content_config,
    )
    return response.text
