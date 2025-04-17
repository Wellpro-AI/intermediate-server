import json
import os
import logging
import asyncio
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import websockets
from google.cloud import firestore
from google.api_core.exceptions import NotFound

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("intermediate")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Consider restricting in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Firestore client (singleton)
db = firestore.AsyncClient()
FIRESTORE_COLLECTION = "session"
active_sessions = {}

def format_time(seconds: float) -> str:
    return f"{int(seconds // 60):02}:{int(seconds % 60):02}"

async def delete_session(session_id: str):
    """Delete session from memory and Firestore"""
    try:
        session = active_sessions.pop(session_id, None)
        if session:
            for conn in session["transcription_conns"].values():
                await conn.close()

            await db.collection(FIRESTORE_COLLECTION).document(session_id).delete()
            logger.info(f"Deleted session {session_id} from Firestore and memory")
    except NotFound:
        logger.warning(f"Session {session_id} not found in Firestore")
    except Exception as e:
        logger.error(f"Error deleting session {session_id}: {e}")

async def schedule_session_deletion(session_id: str):
    await asyncio.sleep(900)  # 15 min
    if session_id in active_sessions:
        session = active_sessions[session_id]
        if session.get("inactive_roles"):
            logger.info(f"Deleting inactive session {session_id}")
            await delete_session(session_id)

async def ping_client(session_id: str, client: WebSocket):
    try:
        while session_id in active_sessions and client in active_sessions[session_id]["clients"]:
            await client.send_text(json.dumps({"type": "ping"}))
            await asyncio.sleep(25)
    except Exception as e:
        logger.warning(f"Ping stopped for session {session_id}: {e}")

async def connect_to_transcription(session_id: str, role: str):
    transcription_url = os.getenv("TRANS_URL")
    if not transcription_url:
        logger.error("TRANS_URL not set in environment")
        return None
    try:
        ws = await asyncio.wait_for(websockets.connect(transcription_url), timeout=10)
        logger.info(f"Transcription connection created for {role} in session {session_id}")
        return ws
    except Exception as e:
        logger.error(f"Failed to connect to transcription service: {e}")
        return None

async def update_conversation_firestore(session_id: str, line: str):
    try:
        await db.collection(FIRESTORE_COLLECTION).document(session_id).update({
            "conversation": firestore.ArrayUnion([line])
        })
    except Exception as e:
        logger.error(f"Error updating Firestore for {session_id}: {e}")

async def transcription_listener(session_id: str, role: str):
    session = active_sessions.get(session_id)
    ws = session["transcription_conns"].get(role)
    if not session or not ws:
        return

    try:
        async for message in ws:
            try:
                data = json.loads(message)
                transcript = data.get("data", "")
                is_final = data.get("is_final", True)
            except Exception:
                transcript = message
                is_final = True

            elapsed = time.time() - session["start_time"]
            formatted_line = f"{format_time(elapsed)} - {role.capitalize()}: {transcript}"
            session["conversation"].append(formatted_line)
            await update_conversation_firestore(session_id, formatted_line)

            payload = json.dumps({
                "type": "transcript",
                "data": formatted_line,
                "sessionId": session_id,
                "is_final": is_final
            })

            # Broadcast to all clients
            for client in list(session["clients"].keys()):
                try:
                    await client.send_text(payload)
                except Exception as e:
                    logger.warning(f"Client send failed: {e}")

    except Exception as e:
        logger.error(f"Listener error for {session_id}-{role}: {e}")
    finally:
        await ws.close()

@app.websocket("/ws/room")
async def room_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = None
    role = None

    try:
        init_data = await websocket.receive_json()
        session_id = init_data.get("sessionId")
        role = init_data.get("role")

        if not session_id or not role:
            await websocket.close(code=4000)
            return

        # Ensure Firestore session document exists
        session_ref = db.collection(FIRESTORE_COLLECTION).document(session_id)
        doc = await session_ref.get()
        if not doc.exists:
            await session_ref.set({
                "start_time": time.time(),
                "conversation": []
            })

        # Initialize in-memory session if not present
        session = active_sessions.setdefault(session_id, {
            "clients": {},
            "transcription_conns": {},
            "conversation": [],
            "start_time": time.time(),
            "inactive_roles": {},
            "deletion_task": None
        })

        # Cancel deletion if reconnecting
        if role in session["inactive_roles"]:
            del session["inactive_roles"][role]
            if session["deletion_task"] and not session["deletion_task"].done():
                session["deletion_task"].cancel()
                session["deletion_task"] = None
                logger.info(f"Cancelled deletion for {session_id}")

        # Create transcription connection if missing
        if role not in session["transcription_conns"]:
            ws = await connect_to_transcription(session_id, role)
            if ws:
                session["transcription_conns"][role] = ws
                asyncio.create_task(transcription_listener(session_id, role))

        # Add client
        session["clients"][websocket] = role
        asyncio.create_task(ping_client(session_id, websocket))

        # Send past conversation
        doc = await session_ref.get()
        convo = doc.to_dict().get("conversation", [])
        if convo:
            await websocket.send_text(json.dumps({
                "type": "transcript",
                "data": "\n".join(convo),
                "sessionId": session_id
            }))

        # Forward audio
        while True:
            audio_data = await websocket.receive_bytes()
            if role in session["transcription_conns"]:
                await session["transcription_conns"][role].send(audio_data)

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {session_id}, role={role}")
    except Exception as e:
        logger.error(f"Unhandled error in /ws/room: {e}")
    finally:
        if session_id in active_sessions:
            session = active_sessions[session_id]
            session["clients"].pop(websocket, None)

            # Role inactive
            if role and not any(r == role for r in session["clients"].values()):
                if role in session["transcription_conns"]:
                    await session["transcription_conns"][role].close()
                    del session["transcription_conns"][role]
                session["inactive_roles"][role] = time.time()

            # Delete if all roles inactive
            if len(session["inactive_roles"]) >= 2:
                await delete_session(session_id)
            elif not session["deletion_task"]:
                session["deletion_task"] = asyncio.create_task(schedule_session_deletion(session_id))

            # Delete if no clients
            if not session["clients"]:
                await delete_session(session_id)
