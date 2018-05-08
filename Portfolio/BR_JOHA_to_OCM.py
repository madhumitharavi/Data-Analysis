import pyodbc
import glob
import csv
import contextlib


"""

@Author : Madhumitha Ravi
@Date : 07-May-2018
@Version : 0.1

This script loads the CSV files into SQL Server, executes Txform and Audit scripts.

##sql_connection = pyodbc.connect('Driver={SQL Server};Server=183.82.251.111,49100\SQLExpress;Database = Py_Joha;UID=madhu;PWD=OcM3214$;')

The above mentioned connection is used t access the sql server and the current script uses a local connection.
Hence, make changes to the connection as needed before executing the script.

We will need to standardize the store procedure execution by taking a backup on all the tables before the script is run as it drops all the existing tables.
Once the tables have been backed up, files should be placed on the server and UNC path should be provided during execution.
For Txform and Audot script execution, the database name must e mentioned before the run, for which the store procedures must be available.
Post standardization, the script will run with no issues.

CHANGE THE DATABASE NAME WITHIN THE SCRIPT POST STANDARDIZATION!!!

"""



## OCM_ALLERGIES


##sql_connection = pyodbc.connect('Driver={SQL Server};Server=183.82.251.111,49100\SQLExpress;Database = Py_Joha;UID=madhu;PWD=OcM3214$;')



# Getting User Input for File locations

Allergies_Path = input('Enter the location of OCM_Allergies file \n')

Allergies_Path_txt = input('Enter the location to place the temporary OCM_Allergies txt file \n')

Billing_Events_Path = input('Enter the location of OCM_BillingEvents file \n')

Billing_Events_Path_txt = input('Enter the location to place the temporary OCM_BillingEvents txt file \n')

FastRxLog_Main_Path = input('Enter the location of OCM_FastRxLog_Main file \n')

FastRxLog_Main_Path_txt = input('Enter the location to place the temporary OCM_FastRxLog_Main txt file \n')

FastRxLog_Morris_Path = input('Enter the location of OCM_FastRxLog_Morris file \n')

FastRxLog_Morris_Path_txt = input('Enter the location to place the temporary OCM_FastRxLog_Morris txt file \n')

Medication_Path = input('Enter the location of OCM_Medication file \n')

Medication_Path_txt = input('Enter the location to place the temporary OCM_Medication txt file \n')

Pathology_Path = input('Enter the location of OCM_Pathology file \n')

Pathology_Path_txt = input('Enter the location to place the temporary OCM_Pathology txt file \n')

Patient_Demographics_Path = input('Enter the location of OCM_Patient_Demographics file \n')

Patient_Demographics_Path_txt = input('Enter the location to place the temporary OCM_Patient_Demographics txt file \n')

PatientDx_Path = input('Enter the location of OCM_PatientDx file \n')

PatientDx_Path_txt = input('Enter the location to place the temporary OCM_PatientDx txt file \n')

Staging_Path = input('Enter the location of OCM_Staging file \n')

Staging_Path_txt = input('Enter the location to place the temporary OCM_Staging txt file \n')

# Assigning the paths

csv_file = Allergies_Path
txt_file = Allergies_Path_txt

csv_file_Billing_Events = Billing_Events_Path
txt_file_Billing_Events = Billing_Events_Path_txt

csv_file_FastRxLog_Main = FastRxLog_Main_Path
txt_file_FastRxLog_Main = FastRxLog_Main_Path_txt

csv_file_FastRxLog_Morris = FastRxLog_Morris_Path
txt_file_FastRxLog_Morris = FastRxLog_Morris_Path_txt

csv_file_Medication = Medication_Path
txt_file_Medication = Medication_Path_txt

csv_file_Pathology = Pathology_Path
txt_file_Pathology = Pathology_Path_txt

csv_file_Patient_Demographics = Patient_Demographics_Path
txt_file_Patient_Demographics = Patient_Demographics_Path_txt

csv_file_PatientDx = PatientDx_Path
txt_file_PatientDx = PatientDx_Path_txt

csv_file_Staging = Staging_Path
txt_file_Staging = Staging_Path_txt

# Connecting to SQL Server

# Local connection is used, server connection should be setup.

sql_connection = pyodbc.connect('Driver={SQL Server};Server=.\SQLExpress;Database = Vim;Trusted_Connection=yes;')

cursor = sql_connection.cursor()

print('Connected to SQL Server \n')

print ('###########################################################################################################################################################')

print('\n')


#########################################################################################################################################################################################

# Converting CSV to TXT

with open(txt_file, "w") as my_output_file:
    with open(csv_file, "r") as my_input_file:
        [ my_output_file.write('\t'.join(row)+'\n') for row in csv.reader(my_input_file)]
    my_output_file.close()

print('\n')
print('Txt Conversion completed')
print('\n')


# Drop existing OCM_Allergies table

dquery = ('Drop TABLE [Py_Joha].[dbo].[OCM_Allergies]')
cursor.execute(dquery)
cursor.commit() 

# Creating OCM_Allergies table

query = ('CREATE TABLE [Py_Joha].[dbo].[OCM_Allergies](	[MRNNo] [float] NULL,	[ReactionType] [nvarchar](255) NULL,	[Allergy] [nvarchar](255) NULL,	[Response] [nvarchar](255) NULL,	[Type] [nvarchar](255) NULL,	[OnSetDate] [datetime] NULL) ON [PRIMARY]')

cursor.execute(query)

cursor.commit() 

print('OCM_Allergies table created')
print('\n')

table_name = ('[Py_Joha].[dbo].[OCM_Allergies]')
file_path = txt_file


# Perform Bulk Insert

string = "BULK INSERT {} FROM '{}' WITH (FIRSTROW=2, FIELDTERMINATOR ='\t',ROWTERMINATOR ='\n' );"
cursor.execute(string.format(table_name, file_path))

count_allergies = "SELECT COUNT(1) from [Py_Joha].[dbo].[OCM_Allergies]"
cursor.execute(count_allergies)
var = cursor.fetchone()

num_lines_temp = sum(1 for line in open(txt_file))
num_lines = num_lines_temp - 1

print ('The number of records inserted read from the source OCM_Allergies file is ' +str(num_lines))
print('\n')
print ('The number of records inserted into OCM_Allergies is ' +str(var[0]))
print('\n')


cursor.commit() 

#########################################################################################################################################################################################

# OCM_BillingEvents


# Converting CSV to TXT

txt_file_Billing_Events = Billing_Events_Path_txt
with open(txt_file_Billing_Events, "w") as my_output_file_Billing_Events:
    with open(csv_file_Billing_Events, "r") as my_input_file_Billing_Events:
        [ my_output_file_Billing_Events.write('\t'.join(row)+'\n') for row in csv.reader(my_input_file_Billing_Events)]
    my_output_file_Billing_Events.close()

print('\n')
print('Txt Conversion completed')
print('\n')

# Drop existing OCM_BillingEvents table

dquery_Billing_Events = ('Drop TABLE [Py_Joha].[dbo].[OCM_BillingEvents]')
cursor.execute(dquery_Billing_Events)
cursor.commit() 

# Creating OCM_BillingEvents table

query_Billing_Events = ('CREATE TABLE [Py_Joha].[dbo].[OCM_BillingEvents]([Institution] [nvarchar](255) NULL,[MRNNo] [float] NULL,[PatLastName] [nvarchar](255) NULL,[PatFirstName] [nvarchar](255) NULL,[PatDOB] [datetime] NULL,[Age] [float] NULL,[AgeCategory] [nvarchar](255) NULL,[EventType] [nvarchar](255) NULL,[EventDate] [datetime] NULL,[BillCode] [nvarchar](255) NULL,[BillEvent] [nvarchar](255) NULL,[Provider] [nvarchar](255) NULL,[NPI] [float] NULL,	[BillStatus] [nvarchar](255) NULL,[VisitId] [float] NULL,[BillEventId] [float] NULL) ON [PRIMARY] ')

cursor.execute(query_Billing_Events)

cursor.commit() 

print('OCM_BillingEvents table created')
print('\n')

table_name_Billing_Events = ('[Py_Joha].[dbo].[OCM_BillingEvents]')
file_path_Billing_Events = txt_file_Billing_Events


# Perform Bulk Insert

string = "BULK INSERT {} FROM '{}' WITH (FIRSTROW=2, FIELDTERMINATOR ='\t',ROWTERMINATOR ='\n' );"
cursor.execute(string.format(table_name_Billing_Events, file_path_Billing_Events))

count_Billing_Events = "SELECT COUNT(1) from [Py_Joha].[dbo].[OCM_BillingEvents]"
cursor.execute(count_Billing_Events)
var_Billing_Events = cursor.fetchone()

num_lines_temp_Billing_Events = sum(1 for line in open(txt_file_Billing_Events))
num_lines_Billing_Events = num_lines_temp_Billing_Events - 1

print ('The number of records inserted read from the source OCM_BillingEvents file is ' +str(num_lines_Billing_Events))
print('\n')
print ('The number of records inserted into OCM_BillingEvents is ' +str(var_Billing_Events[0]))
print('\n')


cursor.commit() 

#########################################################################################################################################################################################

# OCM_FastRxLog_Main


# Converting CSV to TXT

txt_file_FastRxLog_Main = FastRxLog_Main_Path_txt
with open(txt_file_FastRxLog_Main, "w") as my_output_file_FastRxLog_Main:
    with open(csv_file_FastRxLog_Main, "r") as my_input_file_FastRxLog_Main:
        [ my_output_file_FastRxLog_Main.write('\t'.join(row)+'\n') for row in csv.reader(my_input_file_FastRxLog_Main)]
    my_output_file_FastRxLog_Main.close()

print('\n')
print('Txt Conversion completed')
print('\n')

# Drop existing OCM_FastRxLog_Main table

dquery_FastRxLog_Main = ('Drop TABLE [Py_Joha].[dbo].[OCM_FastRxLog_Main]')
cursor.execute(dquery_FastRxLog_Main)
cursor.commit() 

# Creating OCM_FastRxLog_Main table

query_FastRxLog_Main = ('CREATE TABLE [Py_Joha].[dbo].[OCM_FastRxLog_Main](	[FillDate] [datetime] NULL,	[WrittenDate] [datetime] NULL,	[RxNo] [float] NULL,	[NewOrRefillCode] [float] NULL,	[BinNo] [float] NULL,	[InsuranceCode] [nvarchar](255) NULL,	[InsuranceName] [nvarchar](255) NULL,	[PCNNo] [nvarchar](255) NULL,	[DEAClass] [float] NULL,	[DrugName] [nvarchar](255) NULL,	[NDCNo] [float] NULL,	[NoOfRefills] [float] NULL,	[DrugQty] [float] NULL,	[MDLastName] [nvarchar](255) NULL,	[MDFirstName] [nvarchar](255) NULL,	[DEANo] [nvarchar](255) NULL,	[PatLastName] [nvarchar](255) NULL,	[PatFirstName] [nvarchar](255) NULL,	[PatDOB] [datetime] NULL,	[CopayAmt] [float] NULL,	[TotalAmtBilled] [float] NULL,	[SiteName] [nvarchar](255) NULL) ON [PRIMARY]')
cursor.execute(query_FastRxLog_Main)

cursor.commit() 

print('OCM_FastRxLog_Main table created')
print('\n')

table_name_FastRxLog_Main = ('[Py_Joha].[dbo].[OCM_FastRxLog_Main]')
file_path_FastRxLog_Main = txt_file_FastRxLog_Main


# Perform Bulk Insert

string = "BULK INSERT {} FROM '{}' WITH (FIRSTROW=2, FIELDTERMINATOR ='\t',ROWTERMINATOR ='\n' );"
cursor.execute(string.format(table_name_FastRxLog_Main, file_path_FastRxLog_Main))

count_FastRxLog_Main = "SELECT COUNT(1) from [Py_Joha].[dbo].[OCM_FastRxLog_Main]"
cursor.execute(count_FastRxLog_Main)
var_FastRxLog_Main = cursor.fetchone()

num_lines_temp_FastRxLog_Main = sum(1 for line in open(txt_file_FastRxLog_Main))
num_lines_FastRxLog_Main = num_lines_temp_FastRxLog_Main - 1

print ('The number of records inserted read from the source OCM_FastRxLog_Main file is ' +str(num_lines_FastRxLog_Main))
print('\n')
print ('The number of records inserted into OCM_FastRxLog_Main is ' +str(var_FastRxLog_Main[0]))
print('\n')


cursor.commit()


#########################################################################################################################################################################################

# OCM_FastRxLog_Morris


# Converting CSV to TXT

txt_file_FastRxLog_Morris = FastRxLog_Morris_Path_txt
with open(txt_file_FastRxLog_Morris, "w") as my_output_file_FastRxLog_Morris:
    with open(csv_file_FastRxLog_Morris, "r") as my_input_file_FastRxLog_Morris:
        [ my_output_file_FastRxLog_Morris.write('\t'.join(row)+'\n') for row in csv.reader(my_input_file_FastRxLog_Morris)]
    my_output_file_FastRxLog_Morris.close()

print('\n')
print('Txt Conversion completed')
print('\n')

# Drop existing OCM_FastRxLog_Morris table

dquery_FastRxLog_Morris = ('Drop TABLE [Py_Joha].[dbo].[OCM_FastRxLog_Morris]')
cursor.execute(dquery_FastRxLog_Morris)
cursor.commit() 

# Creating OCM_FastRxLog_Morris table

query_FastRxLog_Morris = ('CREATE TABLE [Py_Joha].[dbo].[OCM_FastRxLog_Morris](	[FillDate] [datetime] NULL,	[WrittenDate] [datetime] NULL,	[RxNo] [float] NULL,	[NewOrRefillCode] [float] NULL,	[BinNo] [float] NULL,	[InsuranceCode] [nvarchar](255) NULL,	[InsuranceName] [nvarchar](255) NULL,	[PCNNo] [nvarchar](255) NULL,	[DEAClass] [float] NULL,	[DrugName] [nvarchar](255) NULL,	[NDCNo] [float] NULL,	[NoOfRefills] [float] NULL,	[DrugQty] [float] NULL,	[MDLastName] [nvarchar](255) NULL,	[MDFirstName] [nvarchar](255) NULL,	[DEANo] [nvarchar](255) NULL,	[PatLastName] [nvarchar](255) NULL,	[PatFirstName] [nvarchar](255) NULL,	[PatDOB] [datetime] NULL,	[CopayAmt] [float] NULL,	[TotalAmtBilled] [float] NULL,	[SiteName] [nvarchar](255) NULL) ON [PRIMARY]')
cursor.execute(query_FastRxLog_Morris)

cursor.commit() 

print('OCM_FastRxLog_Morris table created')
print('\n')

table_name_FastRxLog_Morris = ('[Py_Joha].[dbo].[OCM_FastRxLog_Morris]')
file_path_FastRxLog_Morris = txt_file_FastRxLog_Morris


# Perform Bulk Insert

string = "BULK INSERT {} FROM '{}' WITH (FIRSTROW=2, FIELDTERMINATOR ='\t',ROWTERMINATOR ='\n' );"
cursor.execute(string.format(table_name_FastRxLog_Morris, file_path_FastRxLog_Morris))

count_FastRxLog_Morris = "SELECT COUNT(1) from [Py_Joha].[dbo].[OCM_FastRxLog_Morris]"
cursor.execute(count_FastRxLog_Morris)
var_FastRxLog_Morris = cursor.fetchone()

num_lines_temp_FastRxLog_Morris = sum(1 for line in open(txt_file_FastRxLog_Morris))
num_lines_FastRxLog_Morris = num_lines_temp_FastRxLog_Morris - 1

print ('The number of records inserted read from the source OCM_FastRxLog_Morris file is ' +str(num_lines_FastRxLog_Morris))
print('\n')
print ('The number of records inserted into OCM_FastRxLog_Morris is ' +str(var_FastRxLog_Morris[0]))
print('\n')


cursor.commit() 

#########################################################################################################################################################################################


# OCM_Medication


# Converting CSV to TXT

txt_file_Medication = Medication_Path_txt
with open(txt_file_Medication, "w") as my_output_file_Medication:
    with open(csv_file_Medication, "r") as my_input_file_Medication:
        [ my_output_file_Medication.write('\t'.join(row)+'\n') for row in csv.reader(my_input_file_Medication)]
    my_output_file_Medication.close()

print('\n')
print('Txt Conversion completed')
print('\n')

# Drop existing OCM_Medication table

dquery_Medication = ('Drop TABLE [Py_Joha].[dbo].[OCM_Medication]')
cursor.execute(dquery_Medication)
cursor.commit() 

# Creating OCM_Medication table

query_Medication = ('CREATE TABLE [Py_Joha].[dbo].[OCM_Medication](	[MRNNo] [float] NULL,	[Status] [nvarchar](255) NULL,	[Medication] [nvarchar](255) NULL,	[StartDate] [datetime] NULL) ON [PRIMARY]')
cursor.execute(query_Medication)

cursor.commit() 

print('OCM_Medication table created')
print('\n')

table_name_Medication = ('[Py_Joha].[dbo].[OCM_Medication]')
file_path_Medication = txt_file_Medication


# Perform Bulk Insert

string = "BULK INSERT {} FROM '{}' WITH (FIRSTROW=2, FIELDTERMINATOR ='\t',ROWTERMINATOR ='\n' );"
cursor.execute(string.format(table_name_Medication, file_path_Medication))

count_Medication = "SELECT COUNT(1) from [Py_Joha].[dbo].[OCM_Medication]"
cursor.execute(count_Medication)
var_Medication = cursor.fetchone()

num_lines_temp_Medication = sum(1 for line in open(txt_file_Medication))
num_lines_Medication = num_lines_temp_Medication - 1

print ('The number of records inserted read from the source OCM_Medication file is ' +str(num_lines_Medication))
print('\n')
print ('The number of records inserted into OCM_Medication is ' +str(var_Medication[0]))
print('\n')


cursor.commit() 


#########################################################################################################################################################################################


# OCM_Pathology


# Converting CSV to TXT

txt_file_Pathology = Pathology_Path_txt

with open(txt_file_Pathology, "w") as my_output_file_Pathology:
    with open(csv_file_Pathology, "r") as my_input_file_Pathology:
        [ my_output_file_Pathology.write('\t'.join(row)+'^') for row in csv.reader(my_input_file_Pathology)]
    my_output_file_Pathology.close()

print('\n')
print('Txt Conversion completed')
print('\n')

# Drop existing OCM_Pathology table

dquery_Pathology = ('Drop TABLE [Py_Joha].[dbo].[OCM_Pathology]')
cursor.execute(dquery_Pathology)
cursor.commit() 

# Creating OCM_Pathology table

query_Pathology = ('CREATE TABLE [Py_Joha].[dbo].[OCM_Pathology]([MRNNo] [float] NULL,[PtDxId] [float] NULL,	[CellCategory] [nvarchar](255) NULL,	[CellType] [nvarchar](255) NULL,	[CellGrade] [nvarchar](255) NULL,	[InSituCancerInd] [nvarchar](255) NULL,	[GleasonMajor] [nvarchar](255) NULL,	[GleasonMinor] [nvarchar](255) NULL,	[GleasonTertiary] [nvarchar](255) NULL,	[GleasonTotal] [nvarchar](255) NULL,	[Pin] [nvarchar](255) NULL,	[CoresTakenTotal] [nvarchar](255) NULL,	[CoresTakenLeft] [nvarchar](255) NULL,	[CoresTakenRight] [nvarchar](255) NULL,	[CoresPositiveTotal] [nvarchar](255) NULL,	[CoresPositiveLeft] [nvarchar](255) NULL,	[CoresPositiveRight] [nvarchar](255) NULL,	[DiseasePresentApex] [nvarchar](255) NULL,	[DiseasePresentApexLeft] [nvarchar](255) NULL,	[DiseasePCTApexLeft] [nvarchar](255) NULL,	[DiseasePresentApexRight] [nvarchar](255) NULL,	[DiseasePCTApexRight] [nvarchar](255) NULL,	[DiseasePresentMid] [nvarchar](255) NULL,	[DiseasePresentMidLeft] [nvarchar](255) NULL,	[DiseasePCTMidLeft] [nvarchar](255) NULL,	[DiseasePresentMidRight] [nvarchar](255) NULL,	[DiseasePCTMidRight] [nvarchar](255) NULL,	[DiseasePresentBase] [nvarchar](255) NULL,	[DiseasePresentBaseLeft] [nvarchar](255) NULL,	[DiseasePCTBaseLeft] [nvarchar](255) NULL,	[DiseasePresentBaseRight] [nvarchar](255) NULL,	[DiseasePCTBaseRight] [nvarchar](255) NULL,	[TumorMeasurement] [nvarchar](255) NULL,	[LargestFocus] [float] NULL,	[MultiFocal] [nvarchar](255) NULL,	[MarginStatus] [nvarchar](255) NULL,	[MarginLocation] [nvarchar](255) NULL,	[InvasiveTumor] [nvarchar](255) NULL,	[DCIS] [nvarchar](255) NULL,	[Necrosis] [nvarchar](255) NULL,	[Microcalc] [nvarchar](255) NULL,	[NodesExamined] [nvarchar](255) NULL,	[NodesExaminedLeft] [nvarchar](255) NULL,	[NodesExaminedRight] [nvarchar](255) NULL,	[NodesPositive] [nvarchar](255) NULL,	[NodesPositiveLeft] [nvarchar](255) NULL,	[NodesPositiveRight] [nvarchar](255) NULL,	[NodesCytokeratin] [nvarchar](255) NULL,	[LargestNode] [nvarchar](255) NULL,	[ECE] [nvarchar](255) NULL,	[NodeInvolement] [nvarchar](255) NULL,	[ERStatus] [nvarchar](255) NULL,	[PRStatus] [nvarchar](255) NULL,	[SPhase] [nvarchar](255) NULL,	[Ki67Status] [nvarchar](255) NULL,	[Ki67Pct] [float] NULL,	[Her2NeuInd] [nvarchar](255) NULL,	[Her2NeuMethod1] [nvarchar](255) NULL,	[Her2NeuStatus1] [nvarchar](255) NULL,	[Her2NeuMethod2] [nvarchar](255) NULL,	[Her2NeuStatus2] [nvarchar](255) NULL,	[OncotypeDXRecurrenceScore] [nvarchar](255) NULL,	[OncotypeDxRecurrenceRisk] [nvarchar](255) NULL,	[KRAS] [nvarchar](255) NULL,	[Comment] [nvarchar](255) NULL) ON [PRIMARY]')
cursor.execute(query_Pathology)

cursor.commit() 

print('OCM_Pathology table created')
print('\n')

table_name_Pathology = ('[Py_Joha].[dbo].[OCM_Pathology]')
file_path_Pathology = txt_file_Pathology


# Perform Bulk Insert

string = "BULK INSERT {} FROM '{}' WITH (FIRSTROW=2, FIELDTERMINATOR ='\t',ROWTERMINATOR ='^' );"
cursor.execute(string.format(table_name_Pathology, file_path_Pathology))

count_Pathology = "SELECT COUNT(1) from [Py_Joha].[dbo].[OCM_Pathology]"
cursor.execute(count_Pathology)
var_Pathology = cursor.fetchone()

#num_lines_temp_Pathology = sum(1 for line in open(txt_file_Pathology))
#num_lines_Pathology = num_lines_temp_Pathology - 1


with open(txt_file_Pathology) as f:
    contents_Pathology = f.read()
    num_lines_Pathology = contents_Pathology.count("^")

num_lines_Pathology = num_lines_Pathology - 1

print ('The number of records read from the source OCM_Pathology file is ' + str(num_lines_Pathology))
print('\n')
print ('The number of records inserted into OCM_Pathology is ' +str(var_Pathology[0]))
print('\n')


cursor.commit() 


#########################################################################################################################################################################################

# OCM_Patient_Demographics


# Converting CSV to TXT

txt_file_Patient_Demographics = Patient_Demographics_Path_txt

with open(txt_file_Patient_Demographics, "w") as my_output_file_Patient_Demographics:
    with open(csv_file_Patient_Demographics, "r") as my_input_file_Patient_Demographics:
        [ my_output_file_Patient_Demographics.write('\t'.join(row)+'^') for row in csv.reader(my_input_file_Patient_Demographics)]
    my_output_file_Patient_Demographics.close()

print('\n')
print('Txt Conversion completed')
print('\n')

# Drop existing OCM_Patient_Demographics table

dquery_Patient_Demographics = ('Drop TABLE [Py_Joha].[dbo].[OCM_Patient_Demographics]')
cursor.execute(dquery_Patient_Demographics)
cursor.commit() 

# Creating OCM_Patient_Demographics table

query_Patient_Demographics = ('CREATE TABLE [Py_Joha].[dbo].[OCM_Patient_Demographics](	[MRNNo] [float]NULL,	[PatFirstName] [nvarchar](255) NULL,	[PatLastName] [nvarchar](255) NULL,	[PatMI][nvarchar](255) NULL,	[PatDOB] [datetime] NULL,	[Sex] [nvarchar](255) NULL,	[PatStatus][nvarchar](255) NULL,	[DeceasedYN] [nvarchar](255) NULL,	[DateOfDeath] [datetime] NULL,	[CauseofDeath] [nvarchar](255) NULL,	[ClinicalTrialPatient] [nvarchar](255) NULL,	[State][nvarchar](255) NULL,	[Country] [nvarchar](255) NULL,	[Ethnicity] [nvarchar](255) NULL,	[Race] [nvarchar](255) NULL,	[Religion] [nvarchar](255) NULL,	[Language] [nvarchar](255) NULL,	[MaritalStatus] [nvarchar](255) NULL,	[PatientPortal] [nvarchar](255) NULL,	[PrimaryIns][nvarchar](255) NULL,	[Policy] [nvarchar](255) NULL,	[PrimaryYN] [nvarchar](255) NULL,	[PolicyType] [nvarchar](255) NULL,	[Active] [datetime] NULL,	[ClinicalTrialStartDate][nvarchar](255)NULL,	[ClinicalTrialEndDate] [nvarchar](255) NULL,	[HospiseYN] [nvarchar](255) NULL) ON [PRIMARY]')
cursor.execute(query_Patient_Demographics)

cursor.commit() 

print('OCM_Patient_Demographics table created')
print('\n')

table_name_Patient_Demographics = ('[Py_Joha].[dbo].[OCM_Patient_Demographics]')
file_path_Patient_Demographics = txt_file_Patient_Demographics


# Perform Bulk Insert

string = "BULK INSERT {} FROM '{}' WITH (FIRSTROW=2, FIELDTERMINATOR ='\t',ROWTERMINATOR ='^' );"
cursor.execute(string.format(table_name_Patient_Demographics, file_path_Patient_Demographics))

count_Patient_Demographics = "SELECT COUNT(1) from [Py_Joha].[dbo].[OCM_Patient_Demographics]"
cursor.execute(count_Patient_Demographics)
var_Patient_Demographics = cursor.fetchone()

#num_lines_temp_Patient_Demographics = sum(1 for line in open(txt_file_Patient_Demographics))
#num_lines_Patient_Demographics = num_lines_temp_Patient_Demographics - 1


with open(txt_file_Patient_Demographics) as f:
    contents_Patient_Demographics = f.read()
    num_lines_Patient_Demographics = contents_Patient_Demographics.count("^")

num_lines_Patient_Demographics = num_lines_Patient_Demographics - 1

print ('The number of records read from the source OCM_Patient_Demographics file is ' + str(num_lines_Patient_Demographics))
print('\n')
print ('The number of records inserted into OCM_Patient_Demographics is ' +str(var_Patient_Demographics[0]))
print('\n')


cursor.commit()


#########################################################################################################################################################################################

# OCM_PatientDx


# Converting CSV to TXT

txt_file_PatientDx = PatientDx_Path_txt

with open(txt_file_PatientDx, "w") as my_output_file_PatientDx:
    with open(csv_file_PatientDx, "r") as my_input_file_PatientDx:
        [ my_output_file_PatientDx.write('\t'.join(row)+'^') for row in csv.reader(my_input_file_PatientDx)]
    my_output_file_PatientDx.close()

print('\n')
print('Txt Conversion completed')
print('\n')

# Drop existing OCM_PatientDx table

dquery_PatientDx = ('Drop TABLE [Py_Joha].[dbo].[OCM_PatientDx]')
cursor.execute(dquery_PatientDx)
cursor.commit() 

# Creating OCM_PatientDx table

query_PatientDx = ('CREATE TABLE [Py_Joha].[dbo].[OCM_PatientDx]([MRNNo] [float] NULL,[ICD9] [nvarchar](255) NULL,[ICDDescription] [nvarchar](255) NULL,[SNOMED] [float] NULL,[Criteria] [nvarchar](255) NULL,[DXDate] [datetime] NULL,[Status] [nvarchar](255) NULL,[StatusDate] [datetime] NULL,[Ranking] [nvarchar](255) NULL,[DxMethod] [nvarchar](255) NULL,[Source] [nvarchar](255) NULL,[Historic] [nvarchar](255) NULL,[DxId] [float] NULL) ON [PRIMARY]')
cursor.execute(query_PatientDx)

cursor.commit() 

print('OCM_PatientDx table created')
print('\n')

table_name_PatientDx = ('[Py_Joha].[dbo].[OCM_PatientDx]')
file_path_PatientDx = txt_file_PatientDx


# Perform Bulk Insert

string = "BULK INSERT {} FROM '{}' WITH (FIRSTROW=2, FIELDTERMINATOR ='\t',ROWTERMINATOR ='^' );"
cursor.execute(string.format(table_name_PatientDx, file_path_PatientDx))

count_PatientDx = "SELECT COUNT(1) from [Py_Joha].[dbo].[OCM_PatientDx]"
cursor.execute(count_PatientDx)
var_PatientDx = cursor.fetchone()

#num_lines_temp_PatientDx = sum(1 for line in open(txt_file_PatientDx))
#num_lines_PatientDx = num_lines_temp_PatientDx - 1


with open(txt_file_PatientDx) as f:
    contents_PatientDx = f.read()
    num_lines_PatientDx = contents_PatientDx.count("^")

num_lines_PatientDx = num_lines_PatientDx - 1
print ('The number of records read from the source OCM_PatientDx file is ' + str(num_lines_PatientDx))
print('\n')
print ('The number of records inserted into OCM_PatientDx is ' +str(var_PatientDx[0]))
print('\n')


cursor.commit() 


#########################################################################################################################################################################################

# OCM_Staging


# Converting CSV to TXT

txt_file_Staging = Staging_Path_txt

with open(txt_file_Staging, "w") as my_output_file_Staging:
    with open(csv_file_Staging, "r") as my_input_file_Staging:
        [ my_output_file_Staging.write('\t'.join(row)+'^') for row in csv.reader(my_input_file_Staging)]
    my_output_file_Staging.close()

print('\n')
print('Txt Conversion completed')
print('\n')

# Drop existing OCM_Staging table

dquery_Staging = ('Drop TABLE [Py_Joha].[dbo].[OCM_Staging]')
cursor.execute(dquery_Staging)
cursor.commit() 

# Creating OCM_Staging table

query_Staging = ('CREATE TABLE [Py_Joha].[dbo].[OCM_Staging]([MRNNo] [float] NULL,[Stage] [nvarchar](255) NULL,[Criteria] [nvarchar](255) NULL,[CriteriaRow] [nvarchar](255) NULL,[Basis] [nvarchar](255) NULL,[DateStaged] [datetime] NULL,[WorkingStage] [nvarchar](255) NULL,[StageTiming] [nvarchar](255) NULL,[Scheme] [nvarchar](255) NULL,[Status] [nvarchar](255) NULL,[DxId] [float] NULL) ON [PRIMARY]')

cursor.execute(query_Staging)

cursor.commit() 

print('OCM_Staging table created')
print('\n')

table_name_Staging = ('[Py_Joha].[dbo].[OCM_Staging]')
file_path_Staging = txt_file_Staging


# Perform Bulk Insert

string = "BULK INSERT {} FROM '{}' WITH (FIRSTROW=2, FIELDTERMINATOR ='\t',ROWTERMINATOR ='^' );"
cursor.execute(string.format(table_name_Staging, file_path_Staging))

count_Staging = "SELECT COUNT(1) from [Py_Joha].[dbo].[OCM_Staging]"
cursor.execute(count_Staging)
var_Staging = cursor.fetchone()

#num_lines_temp_Staging = sum(1 for line in open(txt_file_Staging))
#num_lines_Staging = num_lines_temp_Staging - 1


with open(txt_file_Staging) as f:
    contents_Staging = f.read()
    num_lines_Staging = contents_Staging.count("^")

num_lines_Staging = num_lines_Staging - 1
print ('The number of records read from the source OCM_Staging file is ' + str(num_lines_Staging))
print('\n')
print ('The number of records inserted into OCM_Staging is ' +str(var_Staging[0]))
print('\n')


cursor.commit() 



# Run Statistics

# Compare the count from source to target


print ('OCM_Allergies Run Statistics')
print('\n')
if( var[0]-num_lines ) == 0:
    print('All records inserted into OCM_Allergies \n')
else:
    print('Not all records inserted into OCM_Allergies, check the data feed \n')
print('\n\n')



# Compare the count from source to target

print ('OCM_BillingEvents Run Statistics')
print('\n')
if( var_Billing_Events[0]-num_lines_Billing_Events ) == 0:
    print('All records inserted into OCM_BillingEvents \n')
else:
    print('Not all records inserted into OCM_BillingEvents, check the data feed \n')
print('\n\n')

# Compare the count from source to target

print ('OCM_FastRxLog_Main Run Statistics')
print('\n')
if( var_FastRxLog_Main[0]-num_lines_FastRxLog_Main ) == 0:
    print('All records inserted into OCM_FastRxLog_Main \n')
else:
    print('Not all records inserted into OCM_FastRxLog_Main, check the data feed \n')
print('\n\n')

# Compare the count from source to target

print ('OCM_FastRxLog_Morris Run Statistics')
print('\n')
if( var_FastRxLog_Morris[0]-num_lines_FastRxLog_Morris ) == 0:
    print('All records inserted into OCM_FastRxLog_Morris \n')
else:
    print('Not all records inserted into OCM_FastRxLog_Morris, check the data feed \n')
print('\n\n')


# Compare the count from source to target

print ('OCM_Medication Run Statistics')
print('\n')
if( var_Medication[0]-num_lines_Medication ) == 0:
    print('All records inserted into OCM_Medication \n')
else:
    print('Not all records inserted into OCM_Medication, check the data feed \n')
print('\n\n')


# Compare the count from source to target

print ('OCM_Pathology Run Statistics')
print('\n')
if( var_Pathology[0]-num_lines_Pathology ) == 0:
    print('All records inserted into OCM_Pathology \n')
else:
    print('Not all records inserted into OCM_Pathology, check the data feed \n')
print('\n\n')


# Compare the count from source to target

print ('OCM_Patient_Demographics Run Statistics')
print('\n')
if( var_Patient_Demographics[0]-num_lines_Patient_Demographics ) == 0:
    print('All records inserted into OCM_Patient_Demographics \n')
else:
    print('Not all records inserted into OCM_Patient_Demographics, check the data feed \n')
print('\n\n')



# Compare the count from source to target

print ('OCM_PatientDx Run Statistics')
print('\n')
if( var_PatientDx[0]-num_lines_PatientDx ) == 0:
    print('All records inserted into OCM_PatientDx \n')
else:
    print('Not all records inserted into OCM_PatientDx, check the data feed \n')
print('\n\n')


# Compare the count from source to target

print ('OCM_Staging Run Statistics')
print('\n')
if( var_Staging[0]-num_lines_Staging ) == 0:
    print('All records inserted into OCM_Staging \n')
else:
    print('Not all records inserted into OCM_Staging, check the data feed \n')
print('\n\n')

"""
#Running Txform scripts

txform1 = "exec dbo.DemographicsTxForm"
cursor.execute(txform1)

txform2 = "exec dbo.PatientDxTransformLogic"
cursor.execute(txform2)

txform3 = "exec dbo.Joha2Ocm_ColonCAncer"
cursor.execute(txform3)

txform4 = "exec dbo.joha2ocm_BreastCancer"
cursor.execute(txform4)

txform5 = "exec dbo.joha2ocm_othercancer"
cursor.execute(txform5)

txform6 = "exec dbo.encounter_Joha2Ocm"
cursor.execute(txform6)


# Running ZeAudit scripts

audit1 = "exec [dbo].[DemographicsAuditLogic] OCMDemo, Demographics"
cursor.execute(audit1)

audit2 = "exec [dbo].[PatientDXAuditLogic]"
cursor.execute(audit2)

audit3 = "exec [dbo].[ColonCancerAuditLogic]"
cursor.execute(audit3)

audit4 = "exec [dbo].[breastcanceraudit]"
cursor.execute(audit4)

audit5 = "exec [dbo].[othercanceraudit]"
cursor.execute(audit5)

audit6 = "exec [dbo].[Ocm2AwsEncounter]"
cursor.execute(audit6)

"""


# Closing the database connection

cursor.close()




