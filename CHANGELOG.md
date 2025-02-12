# Changelog

All notable changes to this project will be documented in this file.

## [1.1.4.post1] - 2025-02-13

### Fixed
- Fixed load null fields from CrossWeb dictionaries

## [1.1.3] - [1.1.4] - 2022-01-24

### Updated
- Updated urls for CrossWeb API: advertisement -> profile, because some browsers block urls with "advertisement" word.
- Added "task_type" in the CrossWeb task and updated send_tasks methods, now they use "task_type" for select API endpoins
- Added checks for available "units" in the Cross Web task

## [1.1.2] - 2022-01-18

### Updated
- Fixed bug with demographic filter in the CrossWeb. (Demographic filter was skipped in the tasks) 


## [1.1.0] - 2021-12-30

### Added
- Added methods for work with CrossWeb API
- Added module for error handling

### Updated
- In the Responsum modules, updated text in comments and docstrings

## [1.0.5] - 2021-06-29

### Updated
- Fixed the facility name for prefact (desktop-pre -> desktop_pre)


## [1.0.4] - 2021-06-23

### Updated
- Updated the wait_task method - added ability to wait for many running tasks

## [1.0.3] - 2021-05-27

### Updated
- Fixed bug with task state in the wait_task method - The method could finish waiting before the task calculated.
- Fixed bug in the result2hierarchy method - added search for names for branches: Agency and Network 

## [1.0.1] - [1.0.2] - 2021-04-27

### Added
- Changelog (this file)
- Added ability to set path to a settings file
- Added ability to set path to a cache folder 
- In a cache filename added user login for ensuring unique cache files for different users

### Updated
- In the Responsum Catalogs module, added parameter "use_cache=True/False" in the get_holdings method.

### Deleted
- Removed setting 'scope': 'offline_access' from token request body, because the auth server don't allowed this option 
  for users
 
