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
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = firestore.AsyncClient(database="transcriber-session-v1")
FIRESTORE_COLLECTION = "session"

active_sessions = {}

def format_time(seconds: float) -> str:
    return f"{int(seconds // 60):02}:{int(seconds % 60):02}"

async def delete_session(session_id: str):
    """Delete session from memory and Firestore"""
    try:
        if session_id in active_sessions:
            # Close all connections
            session = active_sessions[session_id]
            for conn in session["transcription_conns"].values():
                await conn.close()
            
            # Delete from Firestore
            session_ref = db.collection(FIRESTORE_COLLECTION).document(session_id)
            await session_ref.delete()
            logger.info(f"Deleted session {session_id} from Firestore")
            
            # Remove from memory
            del active_sessions[session_id]
            logger.info(f"Removed session {session_id} from memory")
            
    except NotFound:
        logger.warning(f"Session {session_id} not found in Firestore")
    except Exception as e:
        logger.error(f"Error deleting session {session_id}: {e}")

async def schedule_session_deletion(session_id: str):
    """Schedule session deletion after 15 minutes of inactivity"""
    await asyncio.sleep(900)  # 15 minutes
    
    if session_id in active_sessions:
        session = active_sessions[session_id]
        # Check if still has inactive roles
        if session.get("inactive_roles"):
            logger.info(f"Deleting inactive session {session_id}")
            await delete_session(session_id)

async def ping_client(session_id: str, client: WebSocket):
    try:
        while session_id in active_sessions and client in active_sessions[session_id]["clients"]:
            await client.send_text(json.dumps({"type": "ping"}))
            await asyncio.sleep(25)
    except Exception as e:
        logger.error(f"Ping error for session {session_id}: {e}")

async def connect_to_transcription(session_id: str, role: str):
    try:
        transcription_url = os.getenv("TRANS_URL")
        ws = await websockets.connect(transcription_url)
        logger.info(f"Created transcription connection for {role} in session {session_id}")
        return ws
    except Exception as e:
        logger.error(f"Transcription connection failed: {e}")
        return None

async def update_conversation_firestore(session_id: str, line: str):
    try:
        session_ref = db.collection(FIRESTORE_COLLECTION).document(session_id)
        await session_ref.update({
            "conversation": firestore.ArrayUnion([line])
        })
    except Exception as e:
        logger.error(f"Error updating Firestore for session {session_id}: {e}")

async def transcription_listener(session_id: str, role: str):
    session = active_sessions.get(session_id)
    if not session or role not in session["transcription_conns"]:
        return
    ws = session["transcription_conns"][role]
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
            
            for client in list(session["clients"].keys()):
                try:
                    await client.send_text(payload)
                except Exception as e:
                    logger.error(f"Error sending to client: {e}")
    except Exception as e:
        logger.error(f"Transcription listener error for session {session_id}: {e}")
    finally:
        await ws.close()

@app.websocket("/ws/room")
async def room_endpoint(websocket: WebSocket):
    await websocket.accept()
    session_id = None
    role = None
    try:
        init_data = await websocket.receive_json()
        session_id = init_data["sessionId"]
        role = init_data["role"]
        if not session_id or not role:
            await websocket.close(code=4000)
            return
        
        # Initialize Firestore session document if new
        session_ref = db.collection(FIRESTORE_COLLECTION).document(session_id)
        doc = await session_ref.get()
        if not doc.exists:
            await session_ref.set({
                "start_time": time.time(),
                "conversation": []
            })
        
        # Initialize in-memory session mapping if not present
        if session_id not in active_sessions:
            active_sessions[session_id] = {
                "clients": {},
                "transcription_conns": {},
                "conversation": [],
                "start_time": time.time(),
                "inactive_roles": {},
                "deletion_task": None
            }
        
        session = active_sessions[session_id]
        
        # Cancel pending deletion if reconnecting
        if role in session["inactive_roles"]:
            del session["inactive_roles"][role]
            if session["deletion_task"] and not session["deletion_task"].done():
                session["deletion_task"].cancel()
                session["deletion_task"] = None
                logger.info(f"Cancelled deletion task for {session_id}")

        # Create role-specific Deepgram connection if needed
        if role not in session["transcription_conns"]:
            dg_ws = await connect_to_transcription(session_id, role)
            if dg_ws:
                session["transcription_conns"][role] = dg_ws
                asyncio.create_task(transcription_listener(session_id, role))
        
        # Register client
        session["clients"][websocket] = role
        asyncio.create_task(ping_client(session_id, websocket))
        
        # Send conversation history from Firestore
        doc = await session_ref.get()
        session_data = doc.to_dict()
        if session_data and session_data.get("conversation"):
            await websocket.send_text(json.dumps({
                "type": "transcript",
                "data": "\n".join(session_data["conversation"]),
                "sessionId": session_id
            }))
        
        # Audio forwarding loop
        while True:
            audio_data = await websocket.receive_bytes()
            if role in session["transcription_conns"]:
                await session["transcription_conns"][role].send(audio_data)
    
    except WebSocketDisconnect:
        logger.info(f"Client disconnected from session {session_id}")
    except Exception as e:
        logger.error(f"Connection error: {e}")
    finally:
        if session_id and session_id in active_sessions:
            session = active_sessions[session_id]
            session["clients"].pop(websocket, None)
            
            # Check if role became inactive
            role_clients = [c for c, r in session["clients"].items() if r == role]
            if not role_clients:
                # Close transcription connection
                if role in session["transcription_conns"]:
                    await session["transcription_conns"][role].close()
                    del session["transcription_conns"][role]
                
                # Track inactive role
                session["inactive_roles"][role] = time.time()
                logger.info(f"Marked {role} as inactive in {session_id}")
                
                # Immediate cleanup if both roles inactive
                if len(session["inactive_roles"]) >= 2:
                    logger.info(f"All roles inactive - deleting {session_id}")
                    await delete_session(session_id)
                else:
                    # Schedule cleanup after 15 minutes
                    if not session["deletion_task"]:
                        session["deletion_task"] = asyncio.create_task(schedule_session_deletion(session_id))
                        logger.info(f"Scheduled deletion for {session_id} in 15m")
            
            # Cleanup if no clients left
            if not session["clients"]:
                await delete_session(session_id)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)