# Changelog

All notable changes to this project will be documented in this file.

## [1.0.1] - 2021-04-27

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
 
