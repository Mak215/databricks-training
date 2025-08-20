# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Databricks Workspace Walkthrough
# MAGIC
# MAGIC In this walkthrough, we will be covering several key components of the Databricks workspace, including:
# MAGIC
# MAGIC 1. **Explore the Databricks Workspace Homepage** and understand its usefulness for high-level navigation.
# MAGIC
# MAGIC 1. **Utilize the navigation panel** to switch between various Databricks features. 
# MAGIC
# MAGIC 1. **Inspect and change user settings**.
# MAGIC
# MAGIC 1. **Execute the Classroom Setup** for all the demos and labs.
# MAGIC    
# MAGIC 1. **Navigate Workspace assets and objects**, where we will look into how to efficiently move through different areas of the Databricks interface.
# MAGIC
# MAGIC 1. **Work with notebooks**, where we will go over how to create, edit, and run code in notebooks.
# MAGIC
# MAGIC 1. **Unity Catalog and Catalog Explorer**, covering how to assign and manage permissions for different users and groups to ensure secure access to data in addition to how to locate various assets registered to Unity Catalog. 
# MAGIC
# MAGIC 1. **(Optional) Compute**, going over how to use the UI to create a new cluster. 
# MAGIC
# MAGIC 1. **(Optional) Git Functionality**, where we'll discuss how to connect to a git repository and create git folders.
# MAGIC
# MAGIC **Note**: There will be very little coding in this demonstration.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC    - Click **More** in the drop-down.
# MAGIC
# MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explore the Databricks Workspace Homepage
# MAGIC
# MAGIC 1. If you click on the **Databricks logo** at the top left, you'll be taken to a screen that says **Welcome to Databricks**.
# MAGIC
# MAGIC 1. You will see five different tabs labeled:
# MAGIC    - **Recents**
# MAGIC    - **Favorites**
# MAGIC    - **Popular**
# MAGIC    - **Mosaic AI**
# MAGIC    - **What's New**
# MAGIC
# MAGIC 1. Under **Recents**, you'll find all of your notebooks that have been recently opened or worked on.
# MAGIC
# MAGIC 1. Under **Favorites**, anytime you favorite a notebook, table, or any kind of asset within Databricks, you will find it here. To Favorite an item, you'll select the Star icon that will be near the objects name.
# MAGIC
# MAGIC 1. Under **Popular**, this is where you can discover popular tables, notebooks, and other assets within the workspace. These are objects that you have access to that your coworkers have engaged with frequently enough over the last 30 days.
# MAGIC
# MAGIC 1. Under **Mosaic AI**, you will see newly added and featured models registered with Mosaic AI Model Serving. 
# MAGIC
# MAGIC 1. Under **What's New**, you can see recent announcements and updates about the platform. From this screen, you can read more about the feature, check it out within the environment, or create an object directly from the announcement. 
# MAGIC
# MAGIC 1. Additionally, you have the **Search** at the top of the screen where you can search for data, notebooks, or other objects and assets within the platform. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utilize the navigation panel
# MAGIC
# MAGIC
# MAGIC 1. Click on the **hamburger icon** (three horizontal lines stacked on top of each other) at the top left. Clicking on it will reveal or hide the left sidebar navigation. When it is hidden, you can reveal the navigation panel by hovering over the hamburger icon.
# MAGIC
# MAGIC
# MAGIC
# MAGIC 1. In the left sidebar, you will find the following items at the top:
# MAGIC    - **+ New**
# MAGIC    - **Workspace**
# MAGIC    - **Recents**
# MAGIC    - **Catalog**
# MAGIC    - **Jobs and Pipelines**
# MAGIC    - **Compute**
# MAGIC    - **Marketplace**
# MAGIC
# MAGIC 1. If you click on **+ New** at the very top of the sidebar navigation menu, you will have options to create a new:
# MAGIC     - **Add or upload data**
# MAGIC     - **Notebook**
# MAGIC     - **Query** 
# MAGIC     - **Dashboard**
# MAGIC     - **Genie space**
# MAGIC     - **Job**
# MAGIC     - **DLT Pipeline**
# MAGIC     - **Legacy Alert**
# MAGIC     - **Alert Beta**
# MAGIC     - **Experiment**
# MAGIC     - **Model**
# MAGIC     - **App**
# MAGIC     - At the very bottom, you will see **More**, which allows you to create with:
# MAGIC       - **Git folder**
# MAGIC       - **Cluster**
# MAGIC       - **SQL Warehouse**
# MAGIC       - **Serving Endpoint**
# MAGIC
# MAGIC 1. You will also find a grouping of **SQL** menus:
# MAGIC    - **SQL Editor**
# MAGIC    - **Queries**
# MAGIC    - **Dashboards**
# MAGIC    - **Genie**
# MAGIC    - **Alerts**
# MAGIC    - **Query History**
# MAGIC    - **SQL Warehouses**
# MAGIC
# MAGIC 1. Under the **Data Engineering** pane, you will find:
# MAGIC    - **Job Runs**
# MAGIC    - **Data Ingestion**
# MAGIC
# MAGIC 1. Under **AI/ML**, you will find:
# MAGIC    - **Playground**
# MAGIC    - **Agents Beta**
# MAGIC    - **Experiments**
# MAGIC    - **Features**
# MAGIC    - **Models**
# MAGIC    - **Serving**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspect and change user settings
# MAGIC
# MAGIC 1. At the top right, you can click on the user icon, typically a letter associated with your user name, and select **Settings**.
# MAGIC
# MAGIC 2. In **Settings**, you will see **User Settings** with five different options:
# MAGIC    - **Profile**
# MAGIC    - **Preferences**
# MAGIC    - **Developer**
# MAGIC    - **Linked Accounts**
# MAGIC    - **Notifications**
# MAGIC
# MAGIC    **Note** that if you have administrative privileges, you will have an additional set of menu options for **Workspace admin.** Those options are not covered in this content.
# MAGIC
# MAGIC 3. If you click on **Profile**, you will see:
# MAGIC    - Your display name
# MAGIC    - The group(s) you belong to
# MAGIC    - Password for your Databricks Account
# MAGIC
# MAGIC 4. Under **Preferences**, you will see two options to manage:
# MAGIC    - **General: Language**
# MAGIC    - **Appearance: Interface Theme**
# MAGIC
# MAGIC    You can click on these options if you wish to change them.
# MAGIC
# MAGIC 5. Under **Developer**, you will see various options as you scroll down, including:
# MAGIC    - Managing **Access tokens**
# MAGIC    - **SQL Query Snippets**
# MAGIC    - **Editor Settings**
# MAGIC       - **General** settings
# MAGIC       - **Code editor** settings
# MAGIC       - **Experimental features** toggles
# MAGIC
# MAGIC    You'll see various options toggled on or off as you scroll down.
# MAGIC
# MAGIC 6. Under **Linked Accounts**, you will find information about Git integration. Here, you can:
# MAGIC    - Link a Git account
# MAGIC    - Select a **Personal Access Token** for a given Git provider, such as GitHub.
# MAGIC
# MAGIC 7. Finally, under **Notifications**, you can manage when you'll be notified for:
# MAGIC    - **Model Registry** events
# MAGIC    - **Comment email notifications**
# MAGIC    - **Account-level email communications**
# MAGIC    - **Promotional email communications**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Execute the Classroom Setup
# MAGIC ### REQUIRED - SELECT SERVERLESS COMPUTE
# MAGIC Before executing cells in this notebook, please select the available Serverless compute cluster. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the compute cluster for this demonstration:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**, but it is good practice to confirm the correct cluster is selected.
# MAGIC 1. If an alternative compute cluster is selected, use the dropdown to select the **Serverless** compute cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Classroom Setup
# MAGIC
# MAGIC Before continuing the demo, run the provided classroom setup script. This script will define configuration variables necessary for the demo.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1

# COMMAND ----------

# MAGIC %md
# MAGIC **Other Conventions:**
# MAGIC
# MAGIC Throughout this demo, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Dataset Location:  {DA.paths.datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a sample table
# MAGIC
# MAGIC Let's create a sample table using our `DA` object called `wine_quality_table`. We will use this table throughout our demo.

# COMMAND ----------

DA.create_demo_table()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Navigate Workspace assets and objects
# MAGIC
# MAGIC The Workspace area of the platform acts as a central hub for organizing and accessing various assets, such as notebooks and files. Navigate to **Workspace** using the left sidebar menu.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Workspace
# MAGIC
# MAGIC The Workspace is where you'll manage your assets and perform various tasks. Here's how to navigate and perform common actions within the Workspace:
# MAGIC
# MAGIC * **Create a Folder**:
# MAGIC    - Click on the **Workspace** button in the left navigation bar.
# MAGIC    - Select the folder where you want to create a new folder.
# MAGIC    - Right-click and choose **Create > Folder**.
# MAGIC    - Provide a name for the folder and click **Create**
# MAGIC    - Alternatively, click on **Create** button at the top-right corner
# MAGIC    - Select **folder** option name it and click on **Create** button 
# MAGIC
# MAGIC * **Import a File**:
# MAGIC    - Navigate to the desired folder.
# MAGIC    - Right-click and choose **Import**.
# MAGIC    - Select the file you want to import and click **Import**.
# MAGIC    - Alternatively, navigate to desired folder and click on **( ⋮ )** kebab icon at top-right corner
# MAGIC    - Select the import option from the dropdown
# MAGIC    - Select the file you want to import and click **Import**
# MAGIC
# MAGIC * **Export a File**:
# MAGIC    - Right-click on a file in the Workspace.
# MAGIC    - Choose **Download As** and select the desired export format.
# MAGIC    - Alternatively, navigate to folder in which file is present
# MAGIC    - Click on the click on **( ⋮ )** kebab icon and select Download As
# MAGIC    - Select the desired file type
# MAGIC
# MAGIC * **Navigating Folders**:
# MAGIC    - Double-click on a folder to enter it.
# MAGIC    - Use the breadcrumb trail at the top to navigate back.
# MAGIC    - Alternatively, click on the workspace on left sidebar to move at top of folder structure
# MAGIC
# MAGIC * **Create a notebook**:
# MAGIC    - Navigate to the desired folder
# MAGIC    - Right-click and hover over create
# MAGIC    - Select the notebook option from the dropdown
# MAGIC    - Alternatively, click on the **Add** button inside the desired folder
# MAGIC    - Select **notebook** option to create new notebook
# MAGIC
# MAGIC * **Rename Folder**:
# MAGIC    - Navigate over your folder name
# MAGIC    - Right-click on a folder and select **Rename**
# MAGIC    - Enter the new name and select **Ok**
# MAGIC    - Alternatively, navigate to desired folder 
# MAGIC    - Click on kebab icon **( ⋮ )** on folder name want to rename
# MAGIC    - Rename the folder and click on **ok** button
# MAGIC  
# MAGIC * **Share Folder**:
# MAGIC    - Hover over the folder you want to share
# MAGIC    - Click on the kebab icon **( ⋮ )** at right corner
# MAGIC    - Click on the Share (Permissions) option from the dropdown
# MAGIC    - From the dropdown menu, you can select the users you would like to grant permissions to
# MAGIC    - After selection, an additional dropdown menu will appear to the right. 
# MAGIC    - Provide edit, view, manage, run permissions you want to give to user and click **Add**. You will see a message in the top right corner verifying the permission change. 
# MAGIC * **Moving Files**
# MAGIC    - Databricks supports drag and drop when organizing your notebooks and files within **Workspace**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Finding Assets
# MAGIC
# MAGIC In the Workspace, you can quickly find assets using the search bar at the top. The search functionality in Databricks is powered by **DatabricksIQ**, offering intelligent and contextual suggestions as you type.
# MAGIC
# MAGIC **Finding assets through the search bar:**
# MAGIC - Click on the **search bar** at the top-center.
# MAGIC - Type the **name of a known table or notebook** (e.g., `wine_quality_table` if you've created it earlier).
# MAGIC - Select the matching result from the dropdown or press **Enter** to view all matches.
# MAGIC - Use filters to narrow down results by asset type, such as **Notebooks**, **Dashboards**, or **Tables**.
# MAGIC
# MAGIC **Finding recent files and folders:**
# MAGIC - On the left sidebar, click on **Recents**.
# MAGIC - Select the file, notebook, or table you want to revisit.
# MAGIC
# MAGIC > 💡 **Tip:** If you're not sure of the exact name, try a keyword, the search is fuzzy and will still show relevant results.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 6. Work with notebooks
# MAGIC
# MAGIC Let's now explore the power of Databricks notebooks:
# MAGIC
# MAGIC * Attach a Notebook to a Cluster:
# MAGIC    - Click on **Workspace** in the left navigation bar.
# MAGIC    - Select the desired folder or create a new one.
# MAGIC    - Right-click and choose **Create > Notebook**.
# MAGIC    - Name your notebook and select the previously created cluster from the dropdown.
# MAGIC    - Click **Confirm**.
# MAGIC
# MAGIC * Creating a Cell
# MAGIC    - Navigate to the bottom of existing cell.
# MAGIC    - Click on the **(+) Code** icon to add new cell.   
# MAGIC
# MAGIC * Running a Cell:
# MAGIC    - Cells in a notebook can be executed using the **Run** button at the top-left corner of the cell or by pressing **Shift + Enter**.
# MAGIC
# MAGIC * Run all cells:
# MAGIC    - Click on the **Run all** button to run all cells at once in notebook.
# MAGIC
# MAGIC * Create Python, SQL cells:
# MAGIC    - Navigate to the language switcher cell at the top-right of cell.
# MAGIC    - Select the desired language for your cell.
# MAGIC    - Alternatively, type **%py** or **%sql** at the top of the cell.
# MAGIC
# MAGIC * View cell outputs:
# MAGIC    - Notebooks support creating interactive charts to visualize data. 
# MAGIC    - You can view the schema of a Spark DataFrame as a part of the output as well. 
# MAGIC
# MAGIC ### Left sidebar actions
# MAGIC    - Click on the **Table of contents** icon between the left sidebar and the topmost cell to access notebook table content
# MAGIC    - Click on the folder icon to access **folder** structure of the workspace
# MAGIC    - Navigate to the **Catalog** icon to get a list of available catalogs, schemas, and other assets. 
# MAGIC    - Navigate to **Assistant** (generally available) for viewing code suggestions, diagnosing errors, etc.
# MAGIC
# MAGIC ### Right sidebar actions 
# MAGIC    - Click on the message icon to add **Comments** on existing code
# MAGIC    - Use the **MLflow experiments** icon to create a workspace experiment.
# MAGIC    - Access code versioning history through the **Version history** icon.
# MAGIC    - Get a list of variables used in a notebook by navigating to **Variable** icon.
# MAGIC    - View your **Environment** as well, which shows your Python environment configuration. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Working with Markdown
# MAGIC
# MAGIC Working with Markdown Cells:
# MAGIC - Markdown cells allow you to add formatted text and documentation to your notebook.
# MAGIC - Create a new cell, change its type to Markdown, and enter some text using Markdown syntax.
# MAGIC - Alternatively, type **%md** at the top of cell
# MAGIC
# MAGIC Editing a Markdown cell:
# MAGIC - Double click this cell to begin editing it
# MAGIC - Then hit **`Esc`** to stop editing
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Markdown Example
# MAGIC
# MAGIC # Title One
# MAGIC ## Title Two
# MAGIC ### Title Three
# MAGIC
# MAGIC This is a test of the emergency broadcast system. This is only a test.
# MAGIC
# MAGIC This is text with a **bold** word in it.
# MAGIC
# MAGIC This is text with an *italicized* word in it.
# MAGIC
# MAGIC This is an ordered list
# MAGIC 1. one
# MAGIC 1. two
# MAGIC 1. three
# MAGIC
# MAGIC This is an unordered list
# MAGIC * apples
# MAGIC * peaches
# MAGIC * bananas
# MAGIC
# MAGIC Links/Embedded HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC
# MAGIC Images:
# MAGIC
# MAGIC ![Spark](../Includes/images/apache-spark.png)
# MAGIC
# MAGIC And of course, tables:
# MAGIC
# MAGIC | name   | value |
# MAGIC |--------|-------|
# MAGIC | Yi     | 1     |
# MAGIC | Ali    | 2     |
# MAGIC | Selina | 3     |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Executing SQL Code

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wine_quality_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Executing Python Code

# COMMAND ----------

print("This is a Python cell!!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Using the Databricks Assistant
# MAGIC
# MAGIC Here, we show how to utilize the Databricks assistant to write python code for summing integers 1 through 10. 
# MAGIC
# MAGIC 1. **Copy the following prompt:** Use Python to compute the sum of integers 1 through 10. 
# MAGIC 1. Create a new code cell below this one and click the Assistant icon in the top right. Alternatively, you can press **Command/Alt** + **I** on your keyboard. 
# MAGIC 1. Paste the prompt and click **Generate**. 
# MAGIC 1. Click **Run suggested**. The output will be `55`. 
# MAGIC 1. Click the blue **Accept** button to the right.
# MAGIC

# COMMAND ----------

total_sum = sum(range(1, 11))
display(total_sum)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 7. Unity Catalog and Catalog Explorer
# MAGIC
# MAGIC Managing permission is essential for controlling who can access and perform actions on your data and resources. Unity catalog allows data asset owners to manage permissions using the Catalog Explorer UI or using SQL commands.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Granting Table Permissions to Users with the UI
# MAGIC
# MAGIC 1. Navigate to **Catalog** in the sidebar and locate the catalog and schema where your table (e.g., `wine_quality_table`) is stored.
# MAGIC 2. Within the schema, click on the table to open its details page in **Catalog Explorer**.
# MAGIC 3. Go to the **Permissions** tab and click **Grant**.
# MAGIC     - Select the users or groups you want to assign permissions to.
# MAGIC     - Choose the privileges to grant. For example, assign `SELECT` (read) privilege.
# MAGIC     - You may also use **Privilege presets** for broader roles:
# MAGIC         - **Data Reader**: can read any object in the catalog.
# MAGIC         - **Data Editor**: can read, modify, and create new objects in the catalog.
# MAGIC 4. Click **Grant** to apply the changes.
# MAGIC
# MAGIC > 💡 In **Catalog Explorer**, you can also manage other assets like **Volumes** and **Models** associated with your schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Granting Table Permissions to Users Using SQL Statements
# MAGIC
# MAGIC We can also grant permissions via SQL. Run the following cell. You can view the output after running the cell and also verify the result within the Catalog Explorer using the previous instructions.

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE `wine_quality_table` TO `account users`;
# MAGIC SHOW GRANTS ON TABLE `wine_quality_table`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Compute (Optional)
# MAGIC
# MAGIC 1. On the left sidebar, click on **Compute**.
# MAGIC
# MAGIC 2. At the top of the Compute page, you will see several tabs:
# MAGIC    - **All-Purpose Compute**
# MAGIC    - **Job Compute**
# MAGIC    - **SQL Warehouses**
# MAGIC    - **Vector Search**
# MAGIC    - **Pools**
# MAGIC    - **Apps**
# MAGIC
# MAGIC 3. Inside the **All-Purpose Compute** tab, click on the **Create with Personal Compute** button at the top-right corner.
# MAGIC
# MAGIC 4. You will now see a screen where you can configure your compute instance. Select an available **cluster policy** from the dropdown — for example, **Personal Compute**, if available.
# MAGIC
# MAGIC 5. You’ll be able to configure options such as:
# MAGIC    - **Single-node** or **Multi-node**
# MAGIC    - **Access Mode** (e.g., **Single User**, **Shared**)
# MAGIC    - **Node Type**
# MAGIC    - **Instance Profile**
# MAGIC    - **Tags**
# MAGIC    - Other advanced options
# MAGIC
# MAGIC 6. In the **Summary** section, you’ll find:
# MAGIC    - Whether **Unity Catalog** is enabled
# MAGIC    - The **Databricks Runtime** version (e.g., ML Runtime)
# MAGIC
# MAGIC 7. For this demo, we are not creating a new cluster. Click **Cancel** to exit the screen.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Understanding Compute and Runtimes in Databricks
# MAGIC
# MAGIC Databricks offers various types of compute for different tasks:
# MAGIC
# MAGIC - **Serverless Compute for Notebooks**
# MAGIC - **Serverless Compute for Jobs**
# MAGIC - **All-Purpose Compute**
# MAGIC - **Jobs Compute**
# MAGIC - **Instance Pools**
# MAGIC - **Serverless SQL Warehouses**
# MAGIC - **Classic SQL Warehouses**
# MAGIC
# MAGIC You can also choose between **CPU** or **GPU** for compute acceleration.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Photon
# MAGIC
# MAGIC Databricks includes **Photon**, a native vectorized query engine for SQL workloads and DataFrames that significantly boosts performance and reduces cost per workload.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Serverless Compute
# MAGIC
# MAGIC **Serverless compute** automatically manages infrastructure, making it easier and faster to scale jobs and interactive workloads.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Databricks Runtimes
# MAGIC
# MAGIC Databricks Runtime includes optimized versions of:
# MAGIC - **Apache Spark**
# MAGIC - **Delta Lake**
# MAGIC - **Popular libraries** (Java, Scala, Python, R)
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### Databricks Machine Learning Runtime
# MAGIC
# MAGIC The ML Runtime provides:
# MAGIC - Pre-installed **ML libraries**
# MAGIC - Built-in **AutoML**
# MAGIC - Optimizations for data science and ML tasks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 9. Git Functionality (Optional)
# MAGIC
# MAGIC The Databricks Workspace allows you to connect your projects with Git repositories. This enables you to collaborate on code, track changes, and easily sync your work between Databricks and Git. This demo will not provide any hands-on exercises with repos other than creating a Git Folder. However, we will go through the motions of using the UI together. 
# MAGIC
# MAGIC Here's how to work with repos:
# MAGIC
# MAGIC **📌 Note:** Before working with Repos user should have git credentials for resources and operations at the Databricks workspace level. Follow this <a href="https://docs.databricks.com/en/repos/repos-setup.html#set-up-databricks-repos" target="_blank">documentation</a> to set git credentials from **User settings**. [Databricks recommends Git folders over legacy Repos](https://docs.databricks.com/en/repos/what-happened-repos.html).
# MAGIC
# MAGIC * **Add a Repo**:
# MAGIC    - Navigate to the **Workspace** from the sidebar.
# MAGIC    - In the **Workspace Home**, Click the **Create** button on the top right.
# MAGIC    - Select **Git folder** from the dropdown.
# MAGIC    - Paste the Git repo URL:  
# MAGIC    [https://github.com/databricks/databricks-ml-examples.git](https://github.com/databricks/databricks-ml-examples.git)
# MAGIC    - Click **Create Git folder**.
# MAGIC    - Navigate back to **Workspace** and click on Repos and find that a folder with your username has been created. Click on it. 
# MAGIC    - You will find the Git folder titles **databricks-ml-examples** has been created along with the folder for this course that starts with **get-started-with-databricks-for-data-engineering**. 
# MAGIC
# MAGIC * **Pull Changes**:
# MAGIC    - Inside a cloned Git folder, right-click on the folder name
# MAGIC    - Select **Git** option from the dropdown
# MAGIC    - Click on the **Pull** button at the top-right corner to update the repo with the latest changes.
# MAGIC    
# MAGIC * **Push Changes**:
# MAGIC    - Inside a cloned repo folder, click on the **Git** button.
# MAGIC    - Select the **branch** in which you want to push the changes.
# MAGIC    - Choose **Push** to send your local changes to the remote repository. Note that we haven't made any changes, so we cannot actually commit any code. 
# MAGIC
# MAGIC * **Commit Changes**:
# MAGIC    - Inside the cloned Git folder, click the Git button.
# MAGIC    - Select the **branch** where you want to make your changes
# MAGIC    - Enter the commit message
# MAGIC    - Choose **Commit** to save your changes along with the commit message. Note we will not follow through with this commit since we are only demonstrating the process.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this demo, we explored essential aspects of working with the workspace. We went through the workspace homepage, how to navigate the sidebar, how to navigate user settings, understanding how demos and labs will be set up using the classroom setup. We explored the workspace and navigation functionality, such as understanding how Git is integrated with Databricks. We explored how to work with notebooks and how to manage permissions with Unity Catalog. There is much more to discuss, but these are just the highlights of the Data Intelligence Platform.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC
