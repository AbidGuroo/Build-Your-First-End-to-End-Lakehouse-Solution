# Exercise 2 - Transform data using Notebooks and Spark clusters 

> [!NOTE]
> Timebox: 75 minutes
> 
> [Back to Agenda](./../README.md#agenda) | [Back to Exercise 1](./../exercise-1/exercise-1.md) | [Up next Exercise 3](./../exercise-3/exercise-3.md)
> #### List of exercises:
> * [Task 2.1 Copilot for notebooks](#task-21-copilot-for-notebooks)
> * [Task 2.2 Different ways to get data from the lakehouse](#task-22-different-ways-to-get-data-from-the-lakehouse)
> * [Task 2.3 Side Loading (local upload) and Load to Delta for CSV file](#task-23-side-loading-local-upload-and-load-to-delta-for-csv-file)
> * [Task 2.4 Import pre-made notebook](#task-24-import-pre-made-notebook)
> * [Task 2.5 Attach the bronze Lakehouse](#task-25-attach-the-bronze-lakehouse)
> * [Task 2.6 Create a silver lakehouse](#task-26-create-a-silver-lakehouse)
> * [Task 2.7 Follow the Notebook](#task-27-follow-the-notebook)
> * [Task 2.8 Automation](#task-28-automation)
> * [Task 2.9 Confirm End Results](#task-29-confirm-end-results)
> * [Task 2.10 Create Gold Lakehouse (required for Exercise 4)](#task-210-create-gold-lakehouse-required-for-exercise-4)



# Context
In this exercise, we will explore data engineering tasks aimed at transforming raw data into a refined silver layer.

By the end of the workshop, we will have completed the implementation of the medallion architecture:
![Data overview](../screenshots/2/intro.png)



> [!NOTE]
> Fabric's intelligent compute resources are dynamically adjusted based on historical usage, peak demands, and current activity levels. With nearly 600 of us today working simultaneously, primarily within the same region, startup times for Spark compute instances may be longer than usual. Typically, our starter pool initiates new Spark sessions in about 10 seconds. However, due to today's high volume, we may transition to the on-demand pool, resulting in wait times of approximately 2 to 3 minutes for some sessions.

---

# Task 2.1 Copilot for notebooks

> [!TIP]
> Take a moment to check your timing and plan to complete Exercise 2. If you are concerned about delays and need a thorough walkthrough of each required exercise, consider skipping Task 2.1 (Copilot for notebooks) and proceed directly to [Task 2.2 Different ways to get data from the lakehouse](#task-22-different-ways-to-get-data-from-the-lakehouse).
> 
> Remember, you can always return to this exercise later.

<details>

<summary> Click <ins>here</ins> to expand the Task 2.1 Copilot for notebooks </summary>


With Copilot for Data Science and Data Engineering, you can chat with an AI assistant that can help you handle your data analysis and visualization tasks. You can ask the Copilot questions about lakehouse tables, Power BI Datasets, or Pandas/Spark dataframes inside notebooks. Copilot answers in natural language or code snippets. Copilot can also generate data-specific code for you, depending on the task. For example, Copilot for Data Science and Data Engineering can generate code for:
* Chart creation 
* Filtering data 
* Applying transformations 
* Machine learning models


Investigate the `green201501` table within your lakehouse, and seek insights about the dataset. Additionally, inquire how to compute the average trip distance and fare amount by each payment type.

## 2.1.1. Accessing Copilot
Select the Copilot icon found in the notebooks ribbon. This action opens the Copilot chat panel and creates a new cell at the top of your notebook. Note: This cell is essential for initializing a Spark session within a Fabric notebook and must be executed for Copilot to function correctly. Future releases may introduce alternative initialization methods.
![Step](../screenshots/2/1.jpg)

## 2.1.2. Get Started with Copilot
When the Copilot panel opens, click `Get Started` to initiate your interaction with the AI assistant.
![Step](../screenshots/2/2.jpg)

## 2.1.3. Library Installation
Copilot will automatically insert a new cell containing the necessary library installation script. Execute this cell by clicking the `Play` button to install the required libraries for Copilot functionalities.
![Step](../screenshots/2/3.jpg)

## 2.1.4. Data Privacy and Security Awareness
Post-installation, you will be presented with a note on data privacy and security. Please read through this to understand how your data is stored and processed. Additionally, guidelines will be provided on how to effectively communicate with Copilot.
![Step](../screenshots/2/4.jpg)

## 2.1.5. Interacting with Copilot
Now, engage with Copilot by exploring various prompts related to your data. Feel free to ask for code snippets, clarification, or paste the generated code into a new notebook cell. This is an opportunity to explore the capabilities and assistance Copilot offers for data science and engineering tasks.

> [!TIP]
> Please feel free to ask Copilot any questions to deepen your understanding of the dataset and enhance our workshop experience.
> Just an example: `Analyze my lakehouse table named green201501 and provide insights about the data`. 

![Step](../screenshots/2/5.jpg)

This quick demonstration aims to highlight the ease of accessing Copilot for insightful data analysis.

> [!IMPORTANT]  
> Copilot is here to support and guide, to "co-pilot", not to control and "pilot". 
> We'll move forward with our structured activities and that documentation, allowing them to serve as our copilot instead of using Copilot directly.

</details>

---

# Task 2.2 Different ways to get data from the lakehouse

This task focuses on various methods for extracting data from the lakehouse into your notebook for analysis. Below are step-by-step instructions to perform this in your notebook.

## 2.2.1. Code Execution Basics
Remember, to execute code within a cell, use CTRL + Enter on Windows or ⌘ + Enter on MacOS. Alternatively, the `Run` icon (▶️) next to the code cell can be used.

## 2.2.2. Extracting Data Using PySpark
Enter the following PySpark code in a new cell in your Fabric notebook. This script will retrieve data from a specified lakehouse table. Make sure to replace `bronzerawdata` and green202301 with your lakehouse and table names if they differ.

> [!TIP]
> This is a repetition of Exercise 1.3.11, but with additional explanation. Skip it if you do not need the code explanation.

```python
df = spark.sql("SELECT * FROM bronzerawdata.green202301 LIMIT 1000")
display(df)
```

Explanation of the code:
`df = spark.sql("SELECT * FROM bronzerawdata.green202301 LIMIT 1000")` - This line of code uses the `spark.sql()` function to run an SQL query on a table called `green202301` located in the lakehouse `bronzerawdata`. The query selects all columns `(*)` from the table and limits the result to the first 1000 rows with the `LIMIT 1000` clause. The result of the query is then stored in a PySpark DataFrame called `df`. `display(df)` - the `display()` function is used to visualize the contents of a DataFrame in a tabular format. In this case, it visualizes the contents of the df DataFrame created in the previous line.


## 2.2.3. Using Multiple Programming Languages in Fabric Notebooks
Fabric Notebooks support various programming languages, including PySpark, Scala, SQL, and R. To switch to SQL, for example, use the %%sql magic command at the beginning of a notebook cell.

![Step](../screenshots/2/6.jpg)

```python
%%sql
SELECT * FROM bronzerawdata.green202301 LIMIT 1000
```

Now, let's execute a specific data selection command. This command filters specific columns from the DataFrame and displays the first five rows:

```python
%%pyspark
df.select("VendorID", "trip_distance", "fare_amount", "tip_amount").show(5)
```

The code `df.select("VendorID", "trip_distance", "fare_amount", "tip_amount").show(5)` is used to display the first five rows of a DataFrame called df, and only the columns named: `vendorID`, `tripDistance`, `fareAmount`, `tipAmount`. This is a useful function when working with large datasets to quickly inspect the data and ensure that it has been loaded correctly.


## 2.2.4. Understanding Data Workflows
When working with large datasets, starting with data retrieval sets the foundation for subsequent data analysis tasks, which may include filtering, sorting, and aggregating data. As you delve deeper, you may encounter more complex data engineering tasks such as cleansing, transformation, and aggregation, essential for advanced data analysis and insights extraction.


---


# Task 2.3 Side Loading (local upload) and Load to Delta for CSV file

We aim to expand the bronze layer by loading additional data. Below is a table that provides an updated view of the types of data we are loading into the bronze layer and the methods we are using for this purpose.
![Data overview](../screenshots/1/data-integration.png)


This set of instructions will guide you through the process of downloading external data and integrating it into your Lakehouse for comprehensive analysis.

## 2.3.1. Downloading Data
Open Link in a New Tab [provided URL](https://raw.githubusercontent.com/ekote/Build-Your-First-End-to-End-Lakehouse-Solution/fabcon/exercise-2/NYC-Taxi-Discounts-Per-Day.csv) and download a CSV file containing information on discounts applied to users on a specific date. This data is vital for comprehensive analysis and is generated for your convenience. 

> [!TIP]
> Download the file to your local machine from this link [Download Discount Data](https://raw.githubusercontent.com/ekote/Build-Your-First-End-to-End-Lakehouse-Solution/fabcon/exercise-2/NYC-Taxi-Discounts-Per-Day.csv).

![Step](../screenshots/2/7.jpg)

## 2.3.2. Uploading Data to the Lakehouse
To integrate this discount data with existing datasets:
* Go to the `Files` section in your Lakehouse. 
* Click on the three dots to access additional options and select the `Upload` button. 
* Choose `Upload Files` from the menu.
![Step](../screenshots/2/10.jpg)

## 2.3.3. File Selection for Upload
Select the recently downloaded file, likely named NYC-Taxi-Discounts-Per-Day.csv, then initiate the upload by clicking the `Upload` button.
![Step](../screenshots/2/11.jpg)

## 2.3.4. Verifying Upload to the Lakehouse
The file should upload within a few seconds. This method provides a straightforward approach to augmenting your Lakehouse data.
![Step](../screenshots/2/12.jpg)

## 2.3.5. Refreshing and Locating the File
Refresh the Lakehouse's `Files` section to view the newly uploaded file. Employ the drag-and-drop feature to move this file into your notebook. This action will generate a cell prepopulated with code, which you can execute to review the new data.
![Step](../screenshots/2/13.jpg)

## 2.3.6. Renaming the Notebook
Assign an appropriate name to your notebook reflecting its purpose, such as `Data Exploration` or `Discount Analysis`, to maintain clarity and organization within your projects.
![Step](../screenshots/2/14.jpg)

## 2.3.7. Switching to Data Engineering View
Finally, transition to the `Data Engineering` tab, adhering to the instructions depicted on-screen, to continue your data analysis journey with the newly integrated datasets.
![Step](../screenshots/2/15.jpg)


[//]: # (![Step]&#40;../media/2/8.jpg&#41;)

---

# Task 2.4 Import pre-made notebook 

> [!NOTE]  
> You can import one or more existing notebooks from your local computer to a Fabric workspace from the Data Engineering or the Data Science homepage. Fabric notebooks recognize the standard Jupyter Notebook .ipynb files, and source files like .py, .scala, and .sql, and create new notebook items accordingly.

## 2.4.1. Importing the Notebook
If you have not downloaded the repository ([Step 16. Download the exercise files](../exercise-0-setup/start.md#16-download-the-exercise-files)), **you can download just a separate notebook. [This screenshot presents the steps to do that.](../screenshots/extra/download-notebook.jpg)**

Ensure you are in the `Data Engineering` context of your Fabric workspace. Then, navigate to your workspace, and select the `New` where you'll find an option to upload notebooks, symbolized by a notebook icon. 

Click this icon to open the upload sidebar, similar to how you previously uploaded a file. From here, choose the notebook you've recently downloaded, named [notebook-2.ipynb](https://github.com/ekote/Build-Your-First-End-to-End-Lakehouse-Solution/blob/fabcon/exercise-2/notebook-2.ipynb), and initiate the upload.
![Step](../screenshots/2/importnotebook.jpg)

## 2.4.2. Notification
Once you start the upload, you'll receive a notification indicating that the import of the file is underway. Wait for this process to complete; it typically takes only a few moments.
![Step](../screenshots/2/17.jpg)

## 2.4.3. Accessing the Imported Notebook
After the import completes, locate the newly imported notebook in the `urban-innovation-de{NNN}`, where NNN represents the number assigned to you. Click on the three dots associated with the notebook and select `Open Notebook`. For convenience, you can open the notebook in the background, which will make its icon continuously accessible from the vertical sidebar on the left.
![Step](../screenshots/2/18.jpg)


Congratulations, you've successfully completed the task and enhanced your data engineering capabilities with a pre-made notebook!

---


# Task 2.5 Attach the bronze Lakehouse
This step-by-step guide will help you to integrate your Lakehouse with the pre-made notebook for effective data manipulation and analysis.

## 2.5.1. Accessing Lakehouse Options
In your opened notebook, locate the section referring to Lakehouses, typically shown in the screenshot provided within the notebook. Click on this section to view your Lakehouse options.
![Step](../screenshots/2/19.jpg)

## 2.5.2. Adding a Lakehouse
Within the Lakehouse options, click on the `Add` button to initiate the process of linking a Lakehouse to your notebook.
![Step](../screenshots/2/20.jpg)

## 2.5.3. Selecting the Existing Lakehouse
Choose the option to select an `Existing Lakehouse` from the available choices. After making this selection, click the `Add` button to proceed.
![Step](../screenshots/2/21.jpg)

## 2.5.4. Choosing Your Lakehouse
From the list of available Lakehouses, identify and select your own, named `bronzerawdata`. Be careful to choose the correct one to ensure accurate data analysis. Once confirmed, click `Add` to attach it to your notebook.
![Step](../screenshots/2/22.jpg)

## 2.5.5. Confirmation
Verify that your Lakehouse, `bronzerawdata`, is now correctly linked and visible within your notebook settings. This confirmation ensures that you are all set for executing data-related tasks within the notebook.
![Step](../screenshots/2/23.jpg)


---


# Task 2.6 Create a silver lakehouse
The last task before fully immersing ourselves in data engineering work within the notebook is to create and attach a new Silver Lakehouse. Please follow these steps:

## 2.6.1. Click on the pin icon next to the Default Lakehouse, `bronzerawdata`. Then select `Add Lakehouse`.
![Step](../screenshots/2/24.jpg)

## 2.6.2. Choose `New Lakehouse` and click `Add`.
![Step](../screenshots/2/25.jpg)

## 2.6.3. Follow the naming convention and enter a name for the Lakehouse. The suggested name is `silvercleansed`.
![Step](../screenshots/2/26.jpg)

## 2.6.4. Confirm that your notebook is now linked to two Lakehouses: the default one (bronze) and the newly added one (silver). Once this is verified, we can begin our data engineering work.
![Step](../screenshots/2/27.jpg)

---

# Task 2.7 Follow the Notebook

In this task, follow the notebooks and the code provided, as well as all the instructions written in the code. **Execute all code cells there, and all the steps.**. 

> [!IMPORTANT]
> Fabric Spark enforces a cores-based throttling and queueing mechanism, where users can submit jobs based on the purchased Fabric capacity SKUs. The queueing mechanism is a simple FIFO-based queue, which checks for available job slots and automatically retries the jobs once the capacity has become available. When users submit notebook or lakehouse jobs like Load to Table when their capacity is at its maximum utilization due to concurrent running jobs using all the Spark Vcores available for their purchased Fabric capacity SKU, they're throttled with the message **HTTP Response code 430: Unable to submit this request because all the available capacity is currently being used. The suggested solutions are to cancel a currently running job, increase the available capacity, or try again later.** 

You will complete the 2.7 notebook task once you see the last code cell that will lead you back here to Task 2.8. 

> [!TIP]  
> If you have completed Notebook 2.7 and are seeking advanced challenges, please review the notebook and list all the improvements you would suggest. I have intentionally left a couple of areas with room for improvement. Once you identify those areas, feel free to discuss them with the instructors.
> 
> **Please repeat the exercise from [1.4 Management of Spark Sessions](./../exercise-1/exercise-1.md#task-14-management-of-spark-sessions), and check if you have any additional ongoing and active Spark sessions. If so, please cancel them.**

---

# Task 2.8 Automation 

Congratulations, you have completed the advanced data engineering notebook. Now, let's focus on automation. Considering we have just two tables, imagine the scenario where you need to process 50 tables or 50 different Parquet data sources. In such cases, the most efficient approach is to build and prioritize a data pipeline. This is the aim of the task at hand – to establish automation.


> [!TIP]
> Refresh your lakehouse (use the three dots "..." and then click the refresh button) to double-check if the tables are in the lakehouse.

## Pre-Automation Quality Check
Ensure the following before starting the automation process:
* Lakehouse `bronzerawdata` contains two tables: `green201501` and `green202301`.
* Lakehouse `bronzerawdata` includes one folder in the Files section, named `2023`, created by a shortcut.
* Lakehouse `bronzerawdata` has one file in the Files section: `NYC-Taxi-Discounts-Per-Day.csv`.
* Lakehouse `silvercleansed` consists of three tables: `green2015_avg_fare_per_month`, `green201501_cleansed`, and `green201501_discounts`.

Once all is set, proceed with the automation.

## 2.8.1. **Starting Point**
Navigate to `Home` as depicted in the instructions.
![Step](../screenshots/2/28.jpg)

## 2.8.2. **Data Pipeline Creation**
Click on `Data Pipeline` and name the new pipeline `Bronze2Silver`.
![Step](../screenshots/2/29.jpg)

## 2.8.3. **Pipeline Activity**
Select the `ForEach` activity as shown on the screen.
![Step](../screenshots/2/30.jpg)

## 2.8.4. **General Settings for Each Activity**
Provide a name for each `ForEach` element.
![Step](../screenshots/2/31.jpg)

## 2.8.5. **Pipeline Variables**
Firstly, click on the background pane (first step from the screenshot in the pink rectangle) to see the tab with parameters and variables.

In the pipeline settings tab, navigate to `Variables`. Here, create a new variable named `table_name`, set its type to `Array`, and assign the default value `["green201501", "green202301"]`. **Follow the specific step presented in the screenshot.**

![Step](../screenshots/2/32.jpg)

## 2.8.6. **ForEach Settings**
In the `ForEach` settings, select `Sequential`. To add dynamic content, open the sidebar and select the `table_name` variable. Confirm by clicking `OK`. **Follow the specific step presented in the screenshot.**

![Step](../screenshots/2/33.jpg)

> [!NOTE]  
> Sequential specifies whether the loop should be executed sequentially or in parallel. Maximum of 50 loop iterations can be executed at once in parallel. For example, if you have a ForEach activity iterating over a copy activity with 10 different source and sink datasets with isSequential set to False, all copies are executed at once.
> 
> And this "Sequential" mode may not be the best strategy for this task, especially since we want to run the same notebook for two different tables in parallel. If you arrived at the same conclusion, then that's a very astute observation. Feel free to chat about it with instructors.


## 2.8.7. **Adding Notebook Activity**
Under `Activities`, choose `Notebook`.
![Step](../screenshots/2/34.jpg)

## 2.8.8. **Notebook Settings**
From the General Tab (presented in the screenshot), go the `Settings` tab for the notebook, as illustrated.
![Step](../screenshots/2/35.jpg)

## 2.8.9. **Select Workspace**
Choose your workspace.
![Step](../screenshots/2/36.jpg)

## 2.8.10. **Select Notebook and Base Parameters**
Opt for the `notebook-2` that you uploaded previously.
Add a new parameter named `table_name`, with type `String` and value `@item()`. `@item()` comes again from dynamic content (Pipeline expression builder).

![Step](../screenshots/2/37.jpg)

## 2.8.11. **Validation**
After configuring, click `Validate` to ensure there are no errors. Click on `Run`.
![Step](../screenshots/2/38.jpg)

## 2.8.12. **Execution**
Save the settings and initiate the run by clicking the `Run` button.
![Step](../screenshots/2/39.jpg)

## 2.8.13. **Observation and Optimization**
Note that the execution of the two notebooks occurs sequentially, typically taking two minutes each. However, as these notebooks do not depend on each other, consider modifying the pipeline to run the notebooks in parallel for efficiency.

![Step](../screenshots/2/40.jpg)

> [!TIP]
> (1) Instead of iterating through notebooks with a ForEach loop, you may consider structuring your approach to input various values into a single notebook execution. This can be complemented by coding loops within the notebook itself. 
> 
> (2) Utilize the `mssparkutils.notebook.runMultiple()` method to execute multiple notebooks in parallel, enhancing efficiency and saving time. This method is particularly useful when you do not need to wait for each notebook to complete before starting another. For an introduction and detailed usage, execute `mssparkutils.notebook.help("runMultiple")`. Here’s an illustrative example: 
> `mssparkutils.notebook.runMultiple(["NotebookA", "NotebookB"])`
> 
> Explore more about this feature [here](https://learn.microsoft.com/en-us/fabric/data-engineering/microsoft-spark-utilities#reference-run-multiple-notebooks-in-parallel).

--- 

**Congratulations on completing this significant milestone in data engineering automation! Your skills in automating data processes have now been markedly enhanced.**

---

# Task 2.9 Confirm End Results

Upon completing Exercises 1 and 2, it's crucial to verify the following outcomes in your Lakehouse environments:

## Lakehouse `bronzerawdata` Confirmation:
1. **Tables**: Ensure there are two tables present: `green201501` and `green202301`.
2. **Files Section**: Confirm there is one folder named `2023`, created via the shortcut.
3. **File Existence**: Verify there is one file in the Files section: `NYC-Taxi-Discounts-Per-Day.csv`.

![Step](../screenshots/2/52.jpg)

## Lakehouse `silvercleansed` Confirmation:
1. **Tables**: Check that there are six tables:
   - `green201501_avg_fare_per_month_2015_01`
   - `green201501_cleansed`
   - `green201501_discounts`
   - `green202301_avg_fare_per_month_2023_01`
   - `green202301_cleansed`
   - `green202301_discounts`.

![Step](../screenshots/2/51.jpg)

Review the screenshots provided to compare and confirm the setup in your Lakehouses matches the expected structure.

---

# Task 2.10 Create Gold Lakehouse (required for Exercise 4)

The final task before delving into data science work (Exercise 4) within the notebook is to create a new Gold Lakehouse named `goldcurated`.

Please follow these steps:
## 2.10.1. From the view of all artifacts you have created inside your workspace, click on the `New` button.
![Step](../screenshots/2/60.jpg)

## 2.10.2. Then select `Lakehouse` from the extensive list.
![Step](../screenshots/2/61.jpg)

## 2.10.3. Adhere to the naming convention and enter a name for the Lakehouse. The suggested name is `goldcurated`.
![Step](../screenshots/2/62.jpg)

## 2.10.4. Confirm that your Gold Lakehouse has been created.
![Step](../screenshots/2/63.jpg)



> [!IMPORTANT]
> Once completed, go to [next exercise (Exercise 3)](./../exercise-3/exercise-3.md). If time permits before the next exercise begins, consider continuing with [extra steps](../exercise-extra/extra.md).

