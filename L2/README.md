# LSDS2020
## Large Scale Distributed Systems 2020 repository


2- Implement the Twitter filter using Spark

	- Comando de ejecuci�n Spark en local:
	spark-submit --master local[4] --class upf.edu.spark.TwitterLanguageFilterApp lab2-1.0-SNAPSHOT.jar es ./output-folder C:\Eurovision

3- Benchmark the Spark-based TwitterFilter application on EMR

	- Bucket name: edu.upf.ldsd2021.lab2.grp202.team08 
	- Bucket privado con permisos concedidos de lectura y escritura: 
		David: e0e9ca237624a9431223755fd44d740f443a554e8585a99a378bb9ac23f07fc9

	- Folder input:  s3://edu.upf.ldsd2021.lab2.grp202.team08/input/
	- Folder jars: s3://edu.upf.ldsd2021.lab2.grp202.team08/jars/
	- Folder output: s3://edu.upf.ldsd2021.lab2.grp202.team08/output/

	INFO Benchmark de toda la colecci�n entera de Eurovisi�n:

		- Tweets en Espa�ol (�es�): 509435
		- Total del tiempo del proceso lab1 local: 42:10 min
		- EMR - INFO total process run time: 454 seconds = 7:34 min
 
		- Tweets en Franc�s (�fr�):54909
		- Total del tiempo del proceso lab1 local: 7:00 min.
		- EMR - INFO total process run time: 434 seconds  = 7:14 min

		- Tweets en Ingl�s (�en�):446603
		- Total del tiempo del proceso lab1 local: 42:48 min
		- EMR - INFO total process run time: 448 seconds = 7:28 min



4- Most popular bi-grams in a given language during the 2018 Eurovision Festival:

	-Comando de ejecuci�n Spark en local:
	spark-submit --master local[4] --class upf.edu.spark.TwitterLanguageFilterApp lab2-1.0-SNAPSHOT.jar es ./output-folder C:\Eurovision


	INFO Bi-Grams:

		- N�mero de bi-grams en Espa�ol (�es�) : 490769
		- Los 10 BiGrams m�s populares en Espa�ol (�es�)::

			dela: 3187
			#Eurovision: 2410
			#Eurovision#FinalEurovision: 2403
			queno: 2150
			enel: 2027
			de#Eurovision: 1821
			lacanci�n: 1766
			loque: 1702
			enla: 1661
			ala: 1627
 

		- N�mero de bi-grams en Ingl�s (�en�)  : 931828
		- Los 10 BiGrams m�s populares en Ingl�s (�en�): : 

			ofthe: 5551
			inthe: 4949
			forthe: 4069
			#Eurovision: 3378
			theEurovision: 2908
			Thisis: 2708
			isthe: 2642
			thisis: 2597
			tobe: 2541
			onthe: 2345

		- N�mero de bi-grams en Franc�s (�fr�)  : 154277
		- Los10 BiGrams m�s populares en Franc�s (�fr�) :

			dela: 1254
			laFrance: 952
			#Eurovision: 584
			pourla: 573
			cesoir: 560
			lachanson: 482
			�la: 427
			!#Eurovision: 406
			jesuis: 369
			ya: 316


5 Most Retweeted Tweets for Most Retweeted Users:

	-Comando de ejecuci�n Spark en local:
	spark-submit --master local[4] --class upf.edu.spark.MostRetweetedApp lab2-1.0-SNAPSHOT.jar ./output-folder C:\Eurovision\Eurovision3.json

	Nota: Debido al alto tiempo de espera se recomienda ejecutar con un solo archivo de eurovisi�n. Los datos proporcionados a continuaci�n son de la colecci�n entera de Eurovisi�n.

	INFO - 10 most retweeted users: 
 
		Id User: 3143260474         - Number of retweets: 10809
		Id User: 24679473           - Number of retweets: 10155
		Id User: 15584187           - Number of retweets: 9864
		Id User: 437025093          - Number of retweets: 9678
		Id User: 39538010           - Number of retweets: 6831
		Id User: 38381308           - Number of retweets: 6722
		Id User: 739812492310896640 - Number of retweets: 5291
		Id User: 1501434991         - Number of retweets: 5167
		Id User: 29056256           - Number of retweets: 4643
		Id User: 2754746065         - Number of retweets: 4640
		
		Number of retweeted users: 61100

	INFO - 10 most retweeted tweets:

		Id Tweet: 995356756770467840 - Number of retweets: 10809
		Id Tweet: 995435123351973890 - Number of retweets: 5420
		Id Tweet: 995381560277979136 - Number of retweets: 4643
		Id Tweet: 995406052190445568 - Number of retweets: 4138
		Id Tweet: 995417089656672256 - Number of retweets: 3944
		Id Tweet: 995384555719839744 - Number of retweets: 3668
		Id Tweet: 995451073606406144 - Number of retweets: 2991
		Id Tweet: 995374825353904128 - Number of retweets: 2278
		Id Tweet: 995407527864041473 - Number of retweets: 2127
		Id Tweet: 995388604045316097 - Number of retweets: 2113
		
		Number of retweeted tweets: 116014

	INFO - The most retweeted tweet for the 10 most retweeted users:
	
		Id User 1 : 3143260474         - Id Tweet: 995356756770467840
		Id User 2 : 24679473           - Id Tweet: 995397243426541568
		Id User 3 : 15584187           - Id Tweet: 995433967351283712
		Id User 4 : 437025093          - Id Tweet: 995435123351973890
		Id User 5 : 39538010           - Id Tweet: 995434971765538817
		Id User 6 : 38381308           - Id Tweet: 995388604045316097
		Id User 7 : 739812492310896640 - Id Tweet: 995405811261300740
		Id User 8 : 1501434991         - Id Tweet: 995394150978727936
		Id User 9 : 29056256           - Id Tweet: 995381560277979136
		Id User 10 : 2754746065        - Id Tweet: 995439842107576321


