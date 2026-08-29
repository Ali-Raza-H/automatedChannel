from kokoro import KPipeline as kp
import soundfile as sf
import numpy as np

#Kokoro settings:

pipeline = kp(lang_code = "a")
voice_Speed = 1


def genAudio(text, voice):
    
    generation = pipeline(
        text,
        voice = voice,
        speed = voice_Speed
    )

    audio_chunks = []

    for _, _, audio in generation:
        audio_chunks.append(audio)

    audio = np.concatenate(audio_chunks)

    sf.write("generatedAudio.wav", audio, 24000)

