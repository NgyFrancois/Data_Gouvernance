Fait par : 
François NGY 
Antoine THEISSEN 
Rick Georges YELEUMEU VOGMO

Le projet suit le principe de l’architecture **Médaillon** présenté dans la documentation :  
- Bronze = données brutes non modifiées  
- Silver = données validées, typées, nettoyées  
- Gold = données agrégées prêtes à être utilisées  
- Archive = historique horodaté des anciennes versions  
- Quarantine = données rejetées avec motif de rejet

Il faut exécuter les scripts dans cette ordre : bronze.py => silver.py => gold.py
