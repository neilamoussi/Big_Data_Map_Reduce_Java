# TP1: INITIATION A HADOOP ET MAP-REDUCE


Le fichier en entrée est un fichier log dont les champs sont de la forme suivante:

  - date temps magasin produit coût paiement

Les champs du fichier log  sont séparés par des tabulations 

Les codes du mapper et du reducer sont écrits en JAVA.

# A propos

Lors de ce Tp, Nous avons utilisé la machine virtuelle de Cloudera qui est une machine Linux, sur laquelle est installé le

framework Hadoop , ainsi qu’un grand nombre d’outils de son  écosystème, sont préinstallés.

Nous avons testé le mapper et le reducer en local, ensuite nous avons exécuté le code directement sur HDFS.


# Activité 4

le but est de déterminer le total des ventes par magasin

- le mapper: s'occupe d'extraire les couples  (magasin,coût)

- le reducer: s'occupe de faire la somme de tous les coûts pour un même magasin 



# Activité 5

le but est de donner la liste des ventes par catégorie de produits.

- le mapper: s'occupe d'extraire les couples (item,cost)

- le reducer: s'occupe de faire la somme de tous les coûts pour un même magasin

# Activité 6

le but est de Donner le montant de la vente le plus élevé pour chaque magasin

- le mapper: s'occupe d'extraire les couples  (magasin,coût) 

- le reducer: s'occupe de calculer le montant de vente le plus élevé pour chaque magasin.

# Activité 7

le but est de donner  le  nombre  total  des  ventes  et  la  valeur  totale  des  ventes  de  tous  magasins confondus

- le mapper: s'occupe de calculer le  total des ventes et le totale des montants de  ventes puis de passer le couple (total_vente, somme_montant_ventes) 

- le reducer: s'occupe de calculer la somme de toutes les sommes partielles fournies par le map et va calculer le montant total des ventes et  le nombre de total de vente de tous les magasins

# Tester le code

1-- tester le code en local

a- Insatallation

- Ajouter les jars suivantes au projet:

Hadoop Common 
MapReduce Client Core 
jackson-mapper-asl
jackson-core-asl
slf4j-nop
httpclient

- Etapes 

*spécification des arguments (le fichier log et le repertoire du résultat) à utiliser lors de l'execution dans l'onglet Arguments de la fenetre Run configuration 

*Execusion du programme.

*Récupération du résultat dans le fichier part-r-00000 sous le répertoire précisé comme un argument dans l'etape 1

2-- tester le code sur HDFS

- Exportation du jar du projet 

- Stockage du fichiercde donnée  utilisé en entrée dans un repertoire crée sur HDFS: hadoop fs –put [input file] [output file] 

- Exécution de la commande : hadoop jar [jar du projet] [Nom du point d'entrée main dans le jar] [fichier log] [répertoire du résultat]

- Visualisation du résultat dans le ficher part-r-00000 :  hadoop fs -cat [répertoire du résultat]/part-r-00000

# Remarques

On remarque que l'exécution du code en Java est plus rapide qu'avec Python. Alors que le developpement du code en python est plus rapide à celui en java.
En effet ,En ajava On doit écrire en plus du code de mapper et reducer la classe de driver
permet de spécifier le mapper et reducer lors de l'execusion  du job MapReduce.




