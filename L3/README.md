# LSDS2020
## Large Scale Distributed Systems 2020 repository


3- Stateless: joining a static RDD with a real time stream

	
	Time: 1616437410000 ms
-------------------------------------------
(English,556)
(Spanish; Castilian,159)
(Portuguese,104)
(Arabic,103)
(Turkish,76)
(Japanese,51)
(French,46)
(Thai,39)
(Italian,18)
(Korean,17)
...

-------------------------------------------
Time: 1616437440000 ms
-------------------------------------------
(English,546)
(Spanish; Castilian,138)
(Portuguese,120)
(Arabic,105)
(Turkish,88)
(Japanese,54)
(French,47)
(Thai,44)
(Hindi,19)
(Korean,14)
	
	
	
4- Spark Stateful transformations with windows

-------------------------------------------
Time: 1616437980000 ms       <-BATCH
-------------------------------------------
(545,English)
(164,Spanish; Castilian)
(124,Portuguese)
(114,Arabic)
(65,Turkish)
(63,Japanese)
(52,French)
(39,Thai)
(16,Italian)
(13,Russian)
...

-------------------------------------------
Time: 1616437980000 ms        <-WINDOW 
-------------------------------------------
(765,English)
(225,Spanish; Castilian)
(184,Portuguese)
(161,Arabic)
(88,Turkish)
(82,Japanese)
(69,French)
(54,Thai)
(19,Italian)
(18,Hindi)
...


5- Spark Stateful transformations with state variables

Los 20 usuarios que más han tweeteado desde que ha empezado la aplicación

-------------------------------------------
Time: 1616438550000 ms
-------------------------------------------
(3,Andy_pm19)
(3,JulitoBr20)
(2,WilfredoMiraba2)
(2,Frankli93035861)
(2,YanethC32437267)
(2,SarahiAkyurek)
(2,JacquelineC_RG)
(2,TELMEXSoluciona)
(2,ViggianoMila)
(2,gabrielatk137)
(2,Lucisanchezzz15)
(2,sucresabanero)
(2,aleshabit)
(2,patriave113)
(2,OcandoChavez)
(2,MaiAvendanoP)
(1,PatricioRomero_)
(1,JosefinitaL)
(1,JSBG31)
(1,ATOPONE)


6 DynamoDB

6.1 Writing to Dynamo DB:

Creamos la tabla "LSDS2020-TwitterHashtags" en caso de que no exista. 
Hemos elegido como partition key "hashtag" y como sort key "languange" que forman la primary Key unica.
Se han generado 460 items desde el inicio de la apliacación (5 min).


6.2 Reading from DynamoDB

El top 10 de hashtags en lenguage "en" introducido por el usuario.

****** Top 10 Hasthags*******

{"hashTag":"TigaKameraKualiasJuara","lang":"en","count":7}
{"hashTag":"Yemen","lang":"en","count":5}
{"hashTag":"YemenCantWait","lang":"en","count":4}
{"hashTag":"USADetainsOilShips4","lang":"en","count":4}
{"hashTag":"WorldWaterDay","lang":"en","count":3}
{"hashTag":"PrinceTheActor","lang":"en","count":2}
{"hashTag":"OurUniverseRenjunDay","lang":"en","count":2}
{"hashTag":"HAPPYRENJUNDAY","lang":"en","count":2}
{"hashTag":"WorldWaterDay2021","lang":"en","count":2}
{"hashTag":"???_?????_???","lang":"en","count":2}







	