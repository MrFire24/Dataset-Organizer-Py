DEFAULT_CONFIG = {
    "custom_name": "",

    "files": [],
    "additional_files": [],
    "output_path": "results",
    
    "optimize": True,
    "strip_prefixes": True,
    "confirm_overwrite": True,
    
    "groups": ['coordinate', 'io', 'control', 'weld', 'scanner', 'termo', 'set', 'positioner'],

    # any, self, passive
    "default_change_mode": "any",
    
    "change_modes": {
        "_common":{
            "passive": ["*timestamp"]
        }
    }
}