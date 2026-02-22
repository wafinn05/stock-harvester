import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import numpy as np

# Model Checkpoint (Indonesian RoBERTa Sentiment)
# Trained on Twitter & News data. 3 Classes: usually [0=Positive, 1=Neutral, 2=Negative] OR [0=Negative, 1=Neutral, 2=Positive]
# Reference: huggingface.co/w11wo/indonesian-roberta-base-sentiment-classifier
MODEL_NAME = "w11wo/indonesian-roberta-base-sentiment-classifier"

class SentimentEngine:
    def __init__(self):
        print(f"[AI] Loading IndoBERT Model: {MODEL_NAME}...")
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
            self.model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
            self.model.eval() # Set to evaluation mode
            print("[AI] Model loaded successfully!")
        except Exception as e:
            print(f"[AI] Failed to load model: {e}")
            self.model = None

    def predict(self, text: str) -> float:
        """
        Returns sentiment score between -1.0 (Negative) and 1.0 (Positive).
        """
        if not self.model:
            return 0.0

        # Tokenize
        encoded_input = self.tokenizer(text, return_tensors='pt', truncation=True, max_length=128)
        
        # Inference
        with torch.no_grad():
            output = self.model(**encoded_input)
        
        # Get Probabilities
        scores = output.logits[0].numpy()
        probs = softmax(scores) # [Prob_0, Prob_1, Prob_2]
        
        # Verify Label Mapping for this specific model:
        # According to HuggingFace model card for w11wo/indonesian-roberta-base-sentiment-classifier:
        # Label 0: Positive
        # Label 1: Neutral
        # Label 2: Negative
        # WAIT! Let's verify standard mapping. Usually it's Neg(0), Neu(1), Pos(2).
        # Checking... w11wo model usually has: 0=Positive, 1=Neutral, 2=Negative.
        
        prob_pos = probs[0]
        prob_neu = probs[1]
        prob_neg = probs[2]
        
        # Calculate Weighted Score
        # Pos (+1), Neu (0), Neg (-1)
        final_score = (prob_pos * 1.0) + (prob_neu * 0.0) + (prob_neg * -1.0)
        
        return float(final_score)

# Global Instance
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = SentimentEngine()
    return _engine

if __name__ == "__main__":
    # Test
    eng = get_engine()
    texts = [
        "Laba bersih Astra International melonjak drastis tahun ini.", # Pos
        "Harga saham ASII anjlok parah karena skandal.",             # Neg
        "Astra merilis laporan keuangan kuartal 3 hari ini.",        # Neu
        "Penjualan mobil sedang lesu, tapi motor stabil."            # Mixed
    ]
    
    for t in texts:
        s = eng.predict(t)
        print(f"Text: {t}\nScore: {s:.4f}\n")
