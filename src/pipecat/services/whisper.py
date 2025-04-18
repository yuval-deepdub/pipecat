#
# Copyright (c) 2024–2025, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""This module implements Whisper transcription with a locally-downloaded model."""

import asyncio
from enum import Enum
from typing import AsyncGenerator, Optional

import numpy as np
from loguru import logger

from pipecat.frames.frames import ErrorFrame, Frame, TranscriptionFrame
from pipecat.services.ai_services import SegmentedSTTService
from pipecat.transcriptions.language import Language
from pipecat.utils.time import time_now_iso8601

try:
    from faster_whisper import WhisperModel
except ModuleNotFoundError as e:
    logger.error(f"Exception: {e}")
    logger.error("In order to use Whisper, you need to `pip install pipecat-ai[whisper]`.")
    raise Exception(f"Missing module: {e}")


class Model(Enum):
    """Class of basic Whisper model selection options.

    Available models:
        Multilingual models:
            TINY: Smallest multilingual model
            BASE: Basic multilingual model
            MEDIUM: Good balance for multilingual
            LARGE: Best quality multilingual
            DISTIL_LARGE_V2: Fast multilingual

        English-only models:
            DISTIL_MEDIUM_EN: Fast English-only
    """

    # Multilingual models
    TINY = "tiny"
    BASE = "base"
    MEDIUM = "medium"
    LARGE = "large-v3"
    DISTIL_LARGE_V2 = "Systran/faster-distil-whisper-large-v2"

    # English-only models
    DISTIL_MEDIUM_EN = "Systran/faster-distil-whisper-medium.en"


def language_to_whisper_language(language: Language) -> Optional[str]:
    """Maps pipecat Language enum to Whisper language codes.

    Args:
        language: A Language enum value representing the input language.

    Returns:
        str or None: The corresponding Whisper language code, or None if not supported.

    Note:
        Only includes languages officially supported by Whisper.
    """
    language_map = {
        # Arabic
        Language.AR: "ar",
        Language.AR_AE: "ar",
        Language.AR_BH: "ar",
        Language.AR_DZ: "ar",
        Language.AR_EG: "ar",
        Language.AR_IQ: "ar",
        Language.AR_JO: "ar",
        Language.AR_KW: "ar",
        Language.AR_LB: "ar",
        Language.AR_LY: "ar",
        Language.AR_MA: "ar",
        Language.AR_OM: "ar",
        Language.AR_QA: "ar",
        Language.AR_SA: "ar",
        Language.AR_SY: "ar",
        Language.AR_TN: "ar",
        Language.AR_YE: "ar",
        # Bengali
        Language.BN: "bn",
        Language.BN_BD: "bn",
        Language.BN_IN: "bn",
        # Czech
        Language.CS: "cs",
        Language.CS_CZ: "cs",
        # Danish
        Language.DA: "da",
        Language.DA_DK: "da",
        # German
        Language.DE: "de",
        Language.DE_AT: "de",
        Language.DE_CH: "de",
        Language.DE_DE: "de",
        # Greek
        Language.EL: "el",
        Language.EL_GR: "el",
        # English
        Language.EN: "en",
        Language.EN_AU: "en",
        Language.EN_CA: "en",
        Language.EN_GB: "en",
        Language.EN_HK: "en",
        Language.EN_IE: "en",
        Language.EN_IN: "en",
        Language.EN_KE: "en",
        Language.EN_NG: "en",
        Language.EN_NZ: "en",
        Language.EN_PH: "en",
        Language.EN_SG: "en",
        Language.EN_TZ: "en",
        Language.EN_US: "en",
        Language.EN_ZA: "en",
        # Spanish
        Language.ES: "es",
        Language.ES_AR: "es",
        Language.ES_BO: "es",
        Language.ES_CL: "es",
        Language.ES_CO: "es",
        Language.ES_CR: "es",
        Language.ES_CU: "es",
        Language.ES_DO: "es",
        Language.ES_EC: "es",
        Language.ES_ES: "es",
        Language.ES_GQ: "es",
        Language.ES_GT: "es",
        Language.ES_HN: "es",
        Language.ES_MX: "es",
        Language.ES_NI: "es",
        Language.ES_PA: "es",
        Language.ES_PE: "es",
        Language.ES_PR: "es",
        Language.ES_PY: "es",
        Language.ES_SV: "es",
        Language.ES_US: "es",
        Language.ES_UY: "es",
        Language.ES_VE: "es",
        # Persian
        Language.FA: "fa",
        Language.FA_IR: "fa",
        # Finnish
        Language.FI: "fi",
        Language.FI_FI: "fi",
        # French
        Language.FR: "fr",
        Language.FR_BE: "fr",
        Language.FR_CA: "fr",
        Language.FR_CH: "fr",
        Language.FR_FR: "fr",
        # Hindi
        Language.HI: "hi",
        Language.HI_IN: "hi",
        # Hungarian
        Language.HU: "hu",
        Language.HU_HU: "hu",
        # Indonesian
        Language.ID: "id",
        Language.ID_ID: "id",
        # Italian
        Language.IT: "it",
        Language.IT_IT: "it",
        # Japanese
        Language.JA: "ja",
        Language.JA_JP: "ja",
        # Korean
        Language.KO: "ko",
        Language.KO_KR: "ko",
        # Dutch
        Language.NL: "nl",
        Language.NL_BE: "nl",
        Language.NL_NL: "nl",
        # Polish
        Language.PL: "pl",
        Language.PL_PL: "pl",
        # Portuguese
        Language.PT: "pt",
        Language.PT_BR: "pt",
        Language.PT_PT: "pt",
        # Romanian
        Language.RO: "ro",
        Language.RO_RO: "ro",
        # Russian
        Language.RU: "ru",
        Language.RU_RU: "ru",
        # Slovak
        Language.SK: "sk",
        Language.SK_SK: "sk",
        # Swedish
        Language.SV: "sv",
        Language.SV_SE: "sv",
        # Thai
        Language.TH: "th",
        Language.TH_TH: "th",
        # Turkish
        Language.TR: "tr",
        Language.TR_TR: "tr",
        # Ukrainian
        Language.UK: "uk",
        Language.UK_UA: "uk",
        # Urdu
        Language.UR: "ur",
        Language.UR_IN: "ur",
        Language.UR_PK: "ur",
        # Vietnamese
        Language.VI: "vi",
        Language.VI_VN: "vi",
        # Chinese
        Language.ZH: "zh",
        Language.ZH_CN: "zh",
        Language.ZH_HK: "zh",
        Language.ZH_TW: "zh",
    }
    return language_map.get(language)


class WhisperSTTService(SegmentedSTTService):
    """Class to transcribe audio with a locally-downloaded Whisper model.

    This service uses Faster Whisper to perform speech-to-text transcription on audio
    segments. It supports multiple languages and various model sizes.

    Args:
        model: The Whisper model to use for transcription. Can be a Model enum or string.
        device: The device to run inference on ('cpu', 'cuda', or 'auto').
        compute_type: The compute type for inference ('default', 'int8', 'int8_float16', etc.).
        no_speech_prob: Probability threshold for filtering out non-speech segments.
        language: The default language for transcription.
        **kwargs: Additional arguments passed to SegmentedSTTService.

    Attributes:
        _device: The device used for inference.
        _compute_type: The compute type for inference.
        _no_speech_prob: Threshold for non-speech filtering.
        _model: The loaded Whisper model instance.
        _settings: Dictionary containing service settings.
    """

    def __init__(
        self,
        *,
        model: str | Model = Model.DISTIL_MEDIUM_EN,
        device: str = "auto",
        compute_type: str = "default",
        no_speech_prob: float = 0.4,
        language: Language = Language.EN,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._device: str = device
        self._compute_type = compute_type
        self.set_model_name(model if isinstance(model, str) else model.value)
        self._no_speech_prob = no_speech_prob
        self._model: Optional[WhisperModel] = None

        self._settings = {
            "language": language,
        }

        self._load()

    def can_generate_metrics(self) -> bool:
        """Indicates whether this service can generate metrics.

        Returns:
            bool: True, as this service supports metric generation.
        """
        return True

    def language_to_service_language(self, language: Language) -> Optional[str]:
        """Convert from pipecat Language to Whisper language code.

        Args:
            language: The Language enum value to convert.

        Returns:
            str or None: The corresponding Whisper language code, or None if not supported.
        """
        return language_to_whisper_language(language)

    async def set_language(self, language: Language):
        """Set the language for transcription.

        Args:
            language: The Language enum value to use for transcription.
        """
        logger.info(f"Switching STT language to: [{language}]")
        self._settings["language"] = language

    def _load(self):
        """Loads the Whisper model.

        Note:
            If this is the first time this model is being run,
            it will take time to download from the Hugging Face model hub.
        """
        logger.debug("Loading Whisper model...")
        self._model = WhisperModel(
            self.model_name, device=self._device, compute_type=self._compute_type
        )
        logger.debug("Loaded Whisper model")

    async def run_stt(self, audio: bytes) -> AsyncGenerator[Frame, None]:
        """Transcribes given audio using Whisper.

        Args:
            audio: Raw audio bytes in 16-bit PCM format.

        Yields:
            Frame: Either a TranscriptionFrame containing the transcribed text
                  or an ErrorFrame if transcription fails.

        Note:
            The audio is expected to be 16-bit signed PCM data.
            The service will normalize it to float32 in the range [-1, 1].
        """
        if not self._model:
            logger.error(f"{self} error: Whisper model not available")
            yield ErrorFrame("Whisper model not available")
            return

        await self.start_processing_metrics()
        await self.start_ttfb_metrics()

        # Divide by 32768 because we have signed 16-bit data.
        audio_float = np.frombuffer(audio, dtype=np.int16).astype(np.float32) / 32768.0

        whisper_lang = self.language_to_service_language(self._settings["language"])
        segments, _ = await asyncio.to_thread(
            self._model.transcribe, audio_float, language=whisper_lang
        )
        text: str = ""
        for segment in segments:
            if segment.no_speech_prob < self._no_speech_prob:
                text += f"{segment.text} "

        await self.stop_ttfb_metrics()
        await self.stop_processing_metrics()

        if text:
            logger.debug(f"Transcription: [{text}]")
            yield TranscriptionFrame(text, "", time_now_iso8601(), self._settings["language"])
