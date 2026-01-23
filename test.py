import os
print("Test démarrage OK")
print("Dossier actuel :", os.getcwd())
print("Contenu du dossier :", os.listdir('.'))
try:
    with open('watchlist.txt', 'r', encoding='utf-8') as f:
        print("watchlist.txt trouvé ! Contenu :")
        print(f.read())
except FileNotFoundError:
    print("ERREUR : watchlist.txt introuvable dans", os.getcwd())
except Exception as e:
    print("Autre erreur :", e)
print("Fin du test")
input("Appuie sur Entrée pour quitter...")