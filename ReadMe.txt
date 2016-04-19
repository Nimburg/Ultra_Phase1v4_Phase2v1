Please note that Ultra_results_Interpretation.pdf is the introduction to project and interpretation of preliminary results.

This project is seperated into two distinct phases. You should execute phase1v4 first, then use
the results to perform phase2v1.

This Project is written with:

1. Python 2.7
2. with standard libraries: JSON, Numpy, Pandas
3. with pymysql for interfacing with MySQL
4. require MySQL
5. require sklearn, nltk for Phase2



Before executing the phase1v4, please:

1. create your own data base at MySQL, setup user name and password
2. specify those variables at ./Phase1v4/src/Phase1_Main.py
3. you could reassign new keywords of your own choice
4. it is recommended to collect a bigger data file,
at least more than 1 hour real-world-time if you are interested at phase1v4 only,
a lot more if you are interested at both phases. 
5. you should update the data file name in Phase1_Main.py, 
and place your data file under ./Phase1v4/Data



Before executing the Phase2v1, you should know that phase2v1 has 3 separate stages.

Stage1 is executed by ./Phase2v1/src/Phase2_Stage1_Main.py
Stage1 will concatenate datas for users and tags of each sliding window from Phase1v4 into
a single SQL table for analysis. 
You could specify your desired start and end time point. 
You should specify your Database information in ./Phase2v1/src/Phase2_Stage1_Main.py

Stage2 is executed by ./Phase2v1/src/Phase2_Stage3_Main.py
Stage2 will normalize the the concatenated data, making different variables comparable. 
Again, you should specify your Database information

Stage3 is clustering for user and hash tags by:
./Phase2v1/src/Phase2_Stage4_ML_userV2.py
./Phase2v1/src/Phase2_Stage4_ML_tagV2.py
Again, you should specify your Database information
You could adjust your clustering parameters in the above .py files.
