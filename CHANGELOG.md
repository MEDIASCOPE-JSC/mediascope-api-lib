# Changelog

All notable changes to this project will be documented in this file.

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
 
