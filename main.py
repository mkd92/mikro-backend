import os
import io
import json
import asyncio
import logging
from typing import Optional

import pandas as pd
import httpx
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request, Response, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

META_API_KEY = os.getenv("META_API_KEY")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID")
BUSINESS_ACCOUNT_ID = os.getenv("BUSINESS_ACCOUNT_ID")
WEBHOOK_VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN")

# API Base URL for Meta WhatsApp Cloud API
META_API_URL = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"

app = FastAPI(title="Marketing WhatsApp Backend")

async def send_whatsapp_message(to: str, name: str, image_url: str, template_name: str):
    """
    Sends a WhatsApp message using a media template.
    Structure:
    - Header: Image (link provided)
    - Body: Text parameter (personalized with 'name')
    """
    headers = {
        "Authorization": f"Bearer {META_API_KEY}",
        "Content-Type": "application/json",
    }
    
    # Meta's Business API payload for a media template
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {
                "code": "en_US"
            },
            "components": [
                {
                    "type": "header",
                    "parameters": [
                        {
                            "type": "image",
                            "image": {
                                "link": image_url
                            }
                        }
                    ]
                },
                {
                    "type": "body",
                    "parameters": [
                        {
                            "type": "text",
                            "text": name
                        }
                    ]
                }
            ]
        }
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(META_API_URL, headers=headers, json=payload)
            response.raise_for_status()
            logger.info(f"Message sent successfully to {to}")
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Error sending message to {to}: {e.response.text}")
            return {"error": e.response.text, "status": e.response.status_code}
        except Exception as e:
            logger.error(f"Unexpected error for {to}: {str(e)}")
            return {"error": str(e)}

@app.post("/upload-csv")
async def upload_csv(
    background_tasks: BackgroundTasks,
    template_name: str = Form(...),
    file: UploadFile = File(...)
):
    """
    Endpoint to upload a CSV file and trigger WhatsApp messages.
    CSV columns: phone, name, image_url
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload a CSV.")

    try:
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading CSV: {str(e)}")

    # Basic validation
    required_cols = {"phone", "name", "image_url"}
    if not required_cols.issubset(df.columns):
        raise HTTPException(
            status_code=400, 
            detail=f"Missing columns. CSV must contain: {', '.join(required_cols)}"
        )

    # Trigger messages in the background to avoid timing out the request
    async def process_messages(data_frame: pd.DataFrame, temp_name: str):
        for _, row in data_frame.iterrows():
            await send_whatsapp_message(
                to=str(row['phone']),
                name=str(row['name']),
                image_url=str(row['image_url']),
                template_name=temp_name
            )

    background_tasks.add_task(process_messages, df, template_name)

    return {"message": f"Processing {len(df)} records in the background using template '{template_name}'."}

@app.get("/webhooks")
async def verify_webhook(request: Request):
    """
    GET handler for Meta Webhook verification.
    Requires: hub.mode, hub.verify_token, hub.challenge
    """
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode == "subscribe" and token == WEBHOOK_VERIFY_TOKEN:
        logger.info("Webhook verified successfully.")
        return Response(content=challenge, media_type="text/plain")
    else:
        logger.warning("Webhook verification failed.")
        raise HTTPException(status_code=403, detail="Verification token mismatch")

@app.post("/webhooks")
async def handle_webhook(request: Request):
    """
    POST handler for Meta Webhook.
    Logs delivery and read statuses to the console.
    """
    try:
        payload = await request.json()
        
        # Log status updates
        # Payload structure: entry -> changes -> value -> statuses
        entries = payload.get("entry", [])
        for entry in entries:
            for change in entry.get("changes", []):
                value = change.get("value", {})
                statuses = value.get("statuses", [])
                for status in statuses:
                    recipient_id = status.get("recipient_id")
                    status_type = status.get("status")
                    timestamp = status.get("timestamp")
                    logger.info(f"STATUS UPDATE: Recipient {recipient_id} | Status: {status_type} | Timestamp: {timestamp}")
        
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        # Meta expects a 200 OK even if we have an internal error to avoid retries
        return {"status": "ignored", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    # Cloud providers like Koyeb provide a PORT environment variable
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
