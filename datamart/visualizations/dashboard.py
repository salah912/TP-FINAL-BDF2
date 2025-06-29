import streamlit as st
import pandas as pd
import json

# Titre
st.title("📊 Évaluation du modèle - US Accidents")

# Charger les métriques
try:
    with open("/app/ml_training/logs/metrics.json", "r") as f:
        metrics = json.load(f)
except FileNotFoundError:
    st.error("Fichier metrics.json introuvable. Exécutez `generate_metrics.py` d'abord.")
    st.stop()

# Affichage de l'accuracy
st.metric("🎯 Accuracy", f"{metrics['accuracy'] * 100:.2f}%")

# Affichage de la matrice de confusion
st.subheader("🧩 Matrice de confusion")

# Transformer les données
confusion_df = pd.DataFrame(metrics["confusion_matrix"])
if not confusion_df.empty:
    confusion_df = confusion_df.pivot(index='Severity', columns='prediction', values='count').fillna(0).astype(int)
    st.dataframe(confusion_df.style.background_gradient(cmap="Blues"))
else:
    st.warning("Matrice de confusion vide.")

# Footer
st.caption("Projet : US Accidents - Random Forest Classifier")
