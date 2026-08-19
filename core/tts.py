from kokoro import KPipeline as kp
import soundfile as sf
import numpy as np




def genAudio(text, voice):
    pipeline = kp(lang_code="a")
