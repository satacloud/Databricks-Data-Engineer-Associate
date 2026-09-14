# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

import os
from databricks.sdk.service import jobs, pipelines
from databricks.sdk import WorkspaceClient  

class DAJobConfig:
    '''
    Example
    ------------
    job_tasks = [
        {
            'task_name': 'create_table',
            'file_path': '/01 - Simple DAB/create_table',
            'depends_on': None
        },
        {
            'task_name': 'create_table1',
            'file_path': '/01 - Simple DAB/other_table2',
            'depends_on': [{'task_key': 'create_table'}]
        },
        {
            'task_name': 'create_table3',
            'file_path': '/01 - Simple DAB/other_table2',
            'depends_on': [{'task_key': 'create_table'},{'task_key': 'create_table1'}]
        }
    ]


    myjob = DAJobConfig(job_name='test3',
                        job_tasks=job_tasks,
                        job_parameters=[
                            {'name':'target', 'default':'dev'},
                            {'name':'catalog_name', 'default':'test'}
                        ])
    '''
    def __init__(self, 
                 job_name: str,
                 job_tasks: list[dict],
                 job_parameters: list[dict]):
    
        self.job_name = job_name
        self.job_tasks = job_tasks
        self.job_parameters = job_parameters
        
        ## Connect the Workspace
        self.w = self.get_workspace_client()

        ## Execute methods
        self.check_for_duplicate_job_name(check_job_name=self.job_name)
        print(f'Job name is unique. Creating the job {self.job_name}...')

        self.course_path = self.get_path_one_folder_back()
        self.list_job_tasks = self.create_job_tasks()

        self.create_job(job_tasks = self.list_job_tasks)


    ## Get Workspace client
    def get_workspace_client(self):
        """
        Establishes and returns a WorkspaceClient instance for interacting with the Databricks API.
        This is set when the object is created within self.w

        Returns:
            WorkspaceClient: A client instance to interact with the Databricks workspace.
        """
        w = WorkspaceClient()
        return w


    # Check if the job name already exists, return error if it does.
    def check_for_duplicate_job_name(self, check_job_name: str):
        for job in self.w.jobs.list():
            if job.settings.name == check_job_name:
                test_job_name = False
                assert test_job_name, f'You already have a job with the same name. Please manually delete the job {self.job_name}'                


    ## Store the path of one folder one folder back
    def get_path_one_folder_back(self):
        current_path = os.getcwd()
        print(f'Using the following path to reference the Files: {current_path}/.')
        return current_path


    ## Create the job tasks
    def create_job_tasks(self):
        all_job_tasks = []
        for task in job_tasks:
            if task.get('file_path', False) != False:

                ## Create a list of jobs.TaskDependencies
                task_dependencies = [jobs.TaskDependency(task_key=depend_task['task_key']) for depend_task in task['depends_on']] if task['depends_on'] else None

                ## Create the task
                job_task_File = jobs.Task(task_key=task['task_name'],
                                         notebook_task=jobs.NotebookTask(notebook_path=self.course_path+task['file_path']),
                                         depends_on=task_dependencies,
                                         timeout_seconds=0)
                all_job_tasks.append(job_task_File)

            elif task.get('pipeline_task', False) != False:
                job_task_dlt = jobs.Task(task_key=task['task_name'],
                                         pipeline_task=jobs.PipelineTask(pipeline_id=task['pipeline_id'], full_refresh=True),
                                         timeout_seconds=0)
                all_job_tasks.append(job_task_info)

        return all_job_tasks
    

    def set_job_parameters(self, parameters: dict):

        job_params_list = []
        for param in self.job_parameters:
            job_parameter = jobs.JobParameterDefinition(name=param['name'], default=param['default'])
            job_params_list.append(job_parameter)

        return job_params_list
    

    ## Create final job
    def create_job(self, job_tasks: list[jobs.Task]):
        created_job = self.w.jobs.create(
                name=self.job_name,
                tasks=job_tasks,
                parameters = self.set_job_parameters(self.job_parameters)
            )

# COMMAND ----------

@DBAcademyHelper.add_method
def print_job_config(self, job_name_extension, file_paths, Files, job_tasks=None, check_task_dependencies=False):
    """
    - Prints an HTML output in the cell below with information about the Job Name and Files to use with the path for each user.
    - Method also sets the required job name, job Files, and tasks in the DA object as attributes to use for validation testing.

    Parameters:
        - job_name_extension (str): Specify what you want your job name extension to be: (schema_{job_name_extension}).
        - file_paths (str): Uses the a folder where the executing program lives.
            Example: /Task Files/Lesson 1 Files
        - Files (list[str]) : List of File names in the file_paths folder location
            Example: ['01-reset','02-ingest']
        - job_task_names (dict{'task':[list of dependencies]}) : Dictionary of the required task names for the job and their dependencies as a dictionary. Leave as None if you do not want to check the tasks.
            Example:
                job_tasks={
                    'Ingest_CSV': [],
                    'Create_Invalid_Table': ['Ingest_CSV'],
                    'Create_Valid_Table': ['Ingest_CSV','Create_Invalid_Table']
                }
        - check_task_dependencies (boolean) : Determines whether to check for dependencies in job tasks. Leave as False if you don't want to check the dependencies.

    Returns:
        - Return HTML output with the specified string information
        - Attributes
            - user_required_job_name that holds the required job name
            - user_reuqired_job_Files that holds a list of required Files
            - user_required_job_task_names = job tasks and dependencies
            - check_task_dependencies = value if needs to check for dependencies

    Example:
        DA.print_job_config(
            job_name_extension='Lesson_02',
            file_paths='/Task Files/Lesson 2 Files',
            Files=[
                '2.01 - Ingest CSV',
                '2.02 - Create Invalid Table',
                '2.02 - Create Valid Table'
            ],
            job_tasks={
                'Ingest_CSV': [],
                'Create_Invalid_Table': ['Ingest_CSV'],
                'Create_Valid_Table': ['Ingest_CSV','Create_Invalid_Table']
            },
            check_task_dependencies = True
        )
    """

    ## Get current path of File
    base_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)

    ## Concatenates the schema name of the user with the job name
    required_job_name = f"{job_name_extension}_{self.schema_name}"

    ## Goes back two paths to main course folder
    main_course_folder_path = "/Workspace" + "/".join(base_path.split("/")[:-1]) + file_paths + '/'

    ## Create paths for each File specified in method argument Files(list of Files to use)
    Files_to_print = []
    for i, File in enumerate(Files):
        current_file_path = (f'File #{i + 1}', main_course_folder_path + File)
        Files_to_print.append(current_file_path)


    ## Use the display_config_values function to display the following values as HTML output.
    ## Will list the Job Name and File paths.
    self.display_config_values([
            ('Job Name', required_job_name)
        ] + Files_to_print)
    
    ## Store the current job name, Files to use, tasks and if to check task dependencies.
    self.user_required_job_name = required_job_name
    self.user_required_job_Files = [item[1] for item in Files_to_print]
    self.user_required_job_task_names = job_tasks
    self.check_task_dependencies = check_task_dependencies