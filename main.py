from functions import *
import traceback

def main():
    try:
        print("Exécution de get_conso()")
        get_conso()
        conforme()
        merge_conso()

        print("Exécution de get_base()")
        get_base()

        print("Exécution de get_portefeuille()")
        get_portefeuille()

        print("Exécution de get_tarif()")
        get_tarif()

        print("Exécution de get_entities()")
        get_entities()
        get_entities_unactive()

        print("Exécution de merge_all()")
        merge_all()

        print("Exécution de update_drive()")
        update_drive()

    except Exception as e:
        raise RuntimeError(f"Erreur lors de l'exécution du script principal: {e}")

# Gestion globale des erreurs et envoi d'e-mail de statut
error_message = None

try:
    # Exécuter la fonction principale
    main()
    status = "✅ Success"
    error = "OK"
except Exception as e:
    # Capturer le traceback complet en cas d'échec
    error_message = traceback.format_exc()
    status = "❌ Failed"
    error = error_message

# Envoyer un e-mail avec le statut final
envoi_email(status, error)
