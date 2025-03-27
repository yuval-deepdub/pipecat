import asyncio
import base64
import json
from typing import Any, AsyncGenerator

import websockets
from loguru import logger

from pipecat.frames.frames import (
    Frame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    ErrorFrame,
    StartFrame,
    EndFrame,
    CancelFrame,
    StartInterruptionFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.ai_services import InterruptibleWordTTSService


class DeepdubTTSService(InterruptibleWordTTSService):
    def __init__(
        self,
        *,
        api_key: str,
        voice_prompt_id: str,
        locale: str = "en-US",
        model: str = "dd-etts-1.1",
        sample_rate: int = 24000,
        **kwargs: Any,
    ):
        super().__init__(
            aggregate_sentences=True,
            push_text_frames=False,
            push_stop_frames=True,
            pause_frame_processing=True,
            sample_rate=sample_rate,
            **kwargs,
        )
        self._api_key = api_key
        self._voice_prompt_id = voice_prompt_id
        self._locale = locale
        self._model = model
        self._websocket_url = "wss://wsapi.deepdub.ai/open"
        self._websocket = None
        self._started = False

    async def _connect(self):
        if self._websocket:
            return
        self._websocket = await websockets.connect(
            self._websocket_url,
            extra_headers={"x-api-key": self._api_key},
            max_size=16 * 1024 * 1024,
        )
        logger.debug("✅ Connected to Deepdub WebSocket")

    async def _disconnect(self):
        if self._websocket:
            try:
                await self._websocket.close()
                logger.debug("🔌 Disconnected from Deepdub WebSocket")
            except Exception as e:
                logger.warning(f"⚠️ Error while disconnecting: {e}")
            finally:
                self._websocket = None

    async def run_tts(self, text: str) -> AsyncGenerator[Frame, None]:
        try:
            await self._connect()
            await self.start_ttfb_metrics()
            yield TTSStartedFrame()

            request = {
                "action": "text-to-speech",
                "targetText": text,
                "voicePromptId": self._voice_prompt_id,
                "model": self._model,
                "locale": self._locale,
            }

            await self._websocket.send(json.dumps(request))
            logger.debug(f"📤 Sent TTS request to Deepdub: {text}")

            async for message in self._websocket:
                response = json.loads(message)

                if "data" in response:
                    audio_chunk = base64.b64decode(response["data"])
                    frame = TTSAudioRawFrame(audio_chunk, self.sample_rate, 1)
                    await self.push_frame(frame)

                if response.get("isFinished"):
                    break

        except Exception as e:
            logger.error(f"❌ DeepdubTTSService error: {e}")
            yield ErrorFrame(error=str(e))

        finally:
            await self.stop_ttfb_metrics()
            await self._disconnect()
            yield TTSStoppedFrame()
            yield EndFrame()

    async def start(self, frame: StartFrame):
        await super().start(frame)

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        await self._disconnect()

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        await self._disconnect()

    async def push_frame(self, frame: Frame, direction=FrameDirection.DOWNSTREAM):
        await super().push_frame(frame, direction)
        if isinstance(frame, (TTSStoppedFrame, StartInterruptionFrame)):
            self._started = False

    async def _connect_websocket(self):
        # Not used, but required by base class
        pass

    async def _disconnect_websocket(self):
        # Not used, but required by base class
        pass

    async def _receive_messages(self):
        # Not used, but required by base class
        pass
