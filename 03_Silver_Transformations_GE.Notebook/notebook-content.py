# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d97c119d-5b75-4fd0-9c4e-39e923e438bf",
# META       "default_lakehouse_name": "Landing_silver",
# META       "default_lakehouse_workspace_id": "51b0c4d9-b2de-4601-b821-ebed2c1aab95",
# META       "known_lakehouses": [
# META         {
# META           "id": "d97c119d-5b75-4fd0-9c4e-39e923e438bf"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### 03. Silver Transformations

# MARKDOWN ********************

# #### Data Cleaning

# MARKDOWN ********************

# #### 00. Installation et Configuration de Great Expectations

# CELL ********************

%pip install -q great-expectations

import great_expectations as gx
print(f"Great Expectations version: {gx.__version__}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

today_date = '2026-11-23'
workspace = "Student_dev"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/Landing_to_bronze.Lakehouse/Tables/bronze_data"

from pyspark.sql.functions import col
df = spark.read.format('delta').load(fabric_bronze_path).filter(col('Processing_Date') == str(today_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 01. Handling Duplicates

# CELL ********************

print('Count of rows before deleting duplicates :' , df.count())

df_nodups = df.dropDuplicates()

print('Count of rows before deleting duplicates :' , df_nodups.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 02 - Handle missing or NULL values

# MARKDOWN ********************

# ##### 02. a . Drop rows with missing critical values

# CELL ********************

print('Count before dropping missing criticial data rows : ', df_nodups.count() )

df_dropped = df_nodups.dropna(subset=['Student_ID','Course_ID','Enrollment_Date'])

print('Count After dropping missing criticial data rows : ', df_dropped.count() )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 02. b. Fill rows with default values

# CELL ********************

df_filled = df_dropped.fillna({
                "Age": 0,
                "Gender": "Unknown",
                "Status": "In-progress",
                "Final_Grade": "N/A",
                "Attendance_Rate": 0.0,
                "Time_Spent_on_Course_hrs": 0.0,
                "Assignments_Completed": 0,
                "Quizzes_Completed": 0,
                "Forum_Posts": 0,
                "Messages_Sent": 0,
                "Quiz_Average_Score": 0.0,
                "Assignment_Average_Score": 0.0,
                "Project_Score": 0.0,
                "Extra_Credit": 0.0,
                "Overall_Performance": 0.0,
                "Feedback_Score": 0.0,
                "Parent_Involvement": "Unknown",
                "Demographic_Group": "Unknown",
                "Internet_Access": "Unknown",
                "Learning_Disabilities": "Unknown",
                "Preferred_Learning_Style": "Unknown",
                "Language_Proficiency": "Unknown",
                "Participation_Rate": "Unknown",
                "Completion_Time_Days": 0,
                "Performance_Score": 0.0,
                "Course_Completion_Rate": 0.0,
                "Completion_Date": '12/31/9999'

            })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 03. Standardize Date Formats

# CELL ********************

from pyspark.sql.functions import to_date, col

df_format = df_filled.withColumn("Enrollment_Date", to_date(col("Enrollment_Date"), "M/d/yyyy"))\
         .withColumn("Completion_Date",to_date(col('Completion_Date'), "M/d/yyyy"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 04. Check for Logical consistency
# ##### Completion_Date > Enrollment_Date

# CELL ********************

df_consistent = df_format.filter(col("Completion_Date") >= col("Enrollment_Date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Business Transformations

# MARKDOWN ********************

# #### 01. Completion_Time_Days 
# ##### Logic = Completion_Time_Days = Completion_date - Enrollment_Date

# CELL ********************

#1 - We are subtracting CompletionDate - Enrollment_Date
#2 - We are converting that to integer

from pyspark.sql.functions import col

df_days = df_consistent.withColumn("Completion_Time_Days", (col("Completion_Date") - col("Enrollment_Date")  ).cast('int'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 02. Performance_Score
# ###### Logic : (Quiz_Average_Score * 0.2) + (Assignment_Average_Score * 0.2) + (Project_Score * 0.1)

# CELL ********************

from pyspark.sql.functions import col

df_score = df_days.withColumn("Performance_Score", ( 
                                                    (col('Quiz_Average_Score') * 0.2 ) + 
                                                    (col('Assignment_Average_Score')* 0.2) +
                                                    (col('Project_Score')* 0.1) \
                                                )

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 03. Course_Completion_Rate
# 
# ##### Logic: IF(Completion_Time_Days <= 90, 'On Time', 'Delayed')

# CELL ********************

from pyspark.sql.functions import when,col

df_completion = df_score.withColumn("Course_Completion_Rate", when( col("Completion_Time_Days") <=90 , "On-Time").otherwise('Delayed'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### View out of Cleaned and transformed Data

# CELL ********************

df_completion.createOrReplaceTempView('new_data')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data Quality Validation (Great Expectations)
# ##### Vérification des données nettoyées et transformées (`df_completion`) avant l'écriture dans `LH_Silver.silver_data`

# CELL ********************

# Initialisation d'un contexte Great Expectations éphémère en mémoire (compatible Microsoft Fabric / Spark)
context = gx.get_context(mode="ephemeral")

# Connexion au DataFrame PySpark 'df_completion'
data_source = context.data_sources.add_spark(name="fabric_silver_spark")
data_asset = data_source.add_dataframe_asset(name="lms_silver_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe("silver_batch")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df_completion})

# Création de la suite d'attentes (Expectations)
suite = context.suites.add(gx.ExpectationSuite(name="silver_data_quality_suite"))

# 1. Vérification des clés primaires / colonnes critiques (non nulles)
for col_name in ["Student_ID", "Course_ID", "Enrollment_Date", "Completion_Date", "Processing_Date"]:
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col_name))

# 2. Vérification de l'unicité de la paire (Student_ID, Course_ID) avant le MERGE
suite.add_expectation(
    gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["Student_ID", "Course_ID"])
)

# 3. Vérification des domaines de valeurs autorisées
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="Course_Completion_Rate",
        value_set=["On-Time", "Delayed"]
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="Status",
        value_set=["Completed", "In Progress", "In-progress", "Dropped"]
    )
)

# 4. Vérification des plages numériques cohérentes
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="Age", min_value=0, max_value=120))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="Attendance_Rate", min_value=0.0, max_value=100.0))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="Completion_Time_Days", min_value=0))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="Performance_Score", min_value=0.0, max_value=50.0))

# Exécution de la validation
validation_results = batch.validate(suite)

print(f"Statut global de validation : {'SUCCÈS' if validation_results.success else 'ÉCHEC'}")
for res in validation_results.results:
    status = "OK" if res.success else "ECHEC"
    exp_type = res.expectation_config.type
    col_target = res.expectation_config.kwargs.get("column", res.expectation_config.kwargs.get("column_list"))
    print(f"[{status}] {exp_type} -> {col_target}")

# Décommentez la ligne ci-dessous pour bloquer l'écriture dans Silver en cas d'échec qualité :
# assert validation_results.success, "Échec des contrôles de qualité Great Expectations !"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Creating Silver table and reading Silver_data view

# CELL ********************

fabric_silver_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/Landing_silver.Lakehouse/Tables/silver_data"


try:
    print('Reading data from Silver_data table: ')
    spark.read.format('delta').load(fabric_silver_path).createOrReplaceTempView('silver_data')
except:
    print("No table found , Creating silver_data table: ")
    create_table = f"""
    CREATE TABLE silver_data (
    Student_ID STRING,
    Name STRING,
    Age INTEGER NOT NULL,
    Gender STRING NOT NULL,
    Grade_Level STRING,
    Course_ID STRING,
    Course_Name STRING,
    Enrollment_Date DATE,
    Completion_Date DATE,
    Status STRING NOT NULL,
    Final_Grade STRING NOT NULL,
    Attendance_Rate DOUBLE NOT NULL,
    Time_Spent_on_Course_hrs DOUBLE NOT NULL,
    Assignments_Completed INTEGER NOT NULL,
    Quizzes_Completed INTEGER NOT NULL,
    Forum_Posts INTEGER NOT NULL,
    Messages_Sent INTEGER NOT NULL,
    Quiz_Average_Score DOUBLE NOT NULL,
    Assignment_Scores STRING,
    Assignment_Average_Score DOUBLE NOT NULL,
    Project_Score DOUBLE NOT NULL,
    Extra_Credit DOUBLE NOT NULL,
    Overall_Performance DOUBLE NOT NULL,
    Feedback_Score DOUBLE NOT NULL,
    Parent_Involvement STRING NOT NULL,
    Demographic_Group STRING NOT NULL,
    Internet_Access STRING NOT NULL,
    Learning_Disabilities STRING NOT NULL,
    Preferred_Learning_Style STRING NOT NULL,
    Language_Proficiency STRING NOT NULL,
    Participation_Rate STRING NOT NULL,
    Completion_Time_Days INTEGER,
    Performance_Score DOUBLE NOT NULL,
    Course_Completion_Rate STRING NOT NULL,
    Processing_Date DATE ); """

    spark.sql(create_table);
    spark.read.format('delta').load(fabric_silver_path).createOrReplaceTempView('silver_data')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### UPSERT Logic to write data to Silver_data table

# CELL ********************

sql_statement = f""" MERGE INTO silver_data AS target 
                    USING new_data AS source 
                    ON target.Student_ID = source.Student_ID  AND target.Course_ID = source.Course_ID
                    WHEN MATCHED THEN
                    UPDATE SET
                        target.Name = source.Name,
                        target.Age = source.Age,
                        target.Gender = source.Gender,
                        target.Grade_Level = source.Grade_Level,
                        target.Course_Name = source.Course_Name,
                        target.Enrollment_Date = source.Enrollment_Date,
                        target.Completion_Date = source.Completion_Date,
                        target.Status = source.Status,
                        target.Final_Grade = source.Final_Grade,
                        target.Attendance_Rate = source.Attendance_Rate,
                        target.Time_Spent_on_Course_hrs = source.Time_Spent_on_Course_hrs,
                        target.Assignments_Completed = source.Assignments_Completed,
                        target.Quizzes_Completed = source.Quizzes_Completed,
                        target.Forum_Posts = source.Forum_Posts,
                        target.Messages_Sent = source.Messages_Sent,
                        target.Quiz_Average_Score = source.Quiz_Average_Score,
                        target.Assignment_Scores = source.Assignment_Scores,
                        target.Assignment_Average_Score = source.Assignment_Average_Score,
                        target.Project_Score = source.Project_Score,
                        target.Extra_Credit = source.Extra_Credit,
                        target.Overall_Performance = source.Overall_Performance,
                        target.Feedback_Score = source.Feedback_Score,
                        target.Parent_Involvement = source.Parent_Involvement,
                        target.Demographic_Group = source.Demographic_Group,
                        target.Internet_Access = source.Internet_Access,
                        target.Learning_Disabilities = source.Learning_Disabilities,
                        target.Preferred_Learning_Style = source.Preferred_Learning_Style,
                        target.Language_Proficiency = source.Language_Proficiency,
                        target.Participation_Rate = source.Participation_Rate,
                        target.Completion_Time_Days = source.Completion_Time_Days,
                        target.Performance_Score = source.Performance_Score,
                        target.Course_Completion_Rate = source.Course_Completion_Rate,
                        target.Processing_Date = source.Processing_Date

    WHEN NOT MATCHED THEN
    INSERT (
        Student_ID,Name,Age,Gender,Grade_Level,Course_ID,Course_Name,Enrollment_Date,Completion_Date,Status,Final_Grade,Attendance_Rate,Time_Spent_on_Course_hrs,
        Assignments_Completed,Quizzes_Completed,Forum_Posts,Messages_Sent,Quiz_Average_Score,Assignment_Scores,Assignment_Average_Score,Project_Score,Extra_Credit,
        Overall_Performance,Feedback_Score,Parent_Involvement,Demographic_Group,Internet_Access,Learning_Disabilities,Preferred_Learning_Style,Language_Proficiency,
        Participation_Rate,Completion_Time_Days,Performance_Score,Course_Completion_Rate,Processing_Date)
    VALUES (
        source.Student_ID,
        source.Name,
        source.Age,
        source.Gender,
        source.Grade_Level,
        source.Course_ID,
        source.Course_Name,
        source.Enrollment_Date,
        source.Completion_Date,
        source.Status,
        source.Final_Grade,
        source.Attendance_Rate,
        source.Time_Spent_on_Course_hrs,
        source.Assignments_Completed,
        source.Quizzes_Completed,
        source.Forum_Posts,
        source.Messages_Sent,
        source.Quiz_Average_Score,
        source.Assignment_Scores,
        source.Assignment_Average_Score,
        source.Project_Score,
        source.Extra_Credit,
        source.Overall_Performance,
        source.Feedback_Score,
        source.Parent_Involvement,
        source.Demographic_Group,
        source.Internet_Access,
        source.Learning_Disabilities,
        source.Preferred_Learning_Style,
        source.Language_Proficiency,
        source.Participation_Rate,
        source.Completion_Time_Days,
        source.Performance_Score,
        source.Course_Completion_Rate,
        source.Processing_Date
    ) """

spark.sql(sql_statement).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

failed_checks = [
    f"{r.expectation_config.type} ({r.expectation_config.kwargs.get('column', r.expectation_config.kwargs.get('column_list'))})"
    for r in validation_results.results if not r.success
]

# Renvoie le résultat au pipeline sans faire échouer le notebook
mssparkutils.notebook.exit(json.dumps({
    "dq_success": validation_results.success,
    "failed_checks": failed_checks
}))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM Landing_silver.silver_data LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
