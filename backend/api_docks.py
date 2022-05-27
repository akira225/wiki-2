registration_swag = {
    'summary': "Register a new user",
    'description': "Register a new user by username and password",
    'consumes': "application/json",
    'produces': "application/json",
    'parameters': [
        {
            'name': "username",
            'in': "body",
            'required': True,
            'schema': {
                'type': 'string'
            }
        },
        {
            'name': "password",
            'in': "body",
            'required': True,
            'schema': {
                'type': 'string'
            }
        }
    ],
    'responses': {
        "201": {
            'description': "User has been created successfully",
            'schema': {
                "type": "object",
                'properties': {
                    'username': {
                        "type": 'string'
                    },
                    'token': {
                        "type": 'string'
                    }
                }
            }
        },
        '400': {
            'description': "Error message",
            'schema': {
                "type": "object",
                'properties': {
                    'msg': {
                        "type": 'string'
                    }
                }
            }
        }
    }
}

ban_swag = {
    'summary': "Exclude article from possible pathing",
    'description': "Exclude article from possible pathing",
    'consumes': "text/plain; charset=utf-8",
    'produces': "application/json",
    'parameters': [
        {
            'name': "Authorization",
            'description': 'JWT token',
            'in': "header",
            'required': True,
            'schema': {
                'type': 'string'
            }
        },
        {
            'name': "article",
            'description': 'Wiki article',
            'in': "path",
            'required': True,
            'schema': {
                'type': 'string'
            }
        }
    ],
    'responses': {
        "200": {
            'description': "Success message",
            'schema': {
                "type": "object",
                'properties': {
                    'msg': {
                        "type": 'string'
                    }
                }
            }
        },
        "400": {
            'description': "Error message",
            'schema': {
                "type": "object",
                'properties': {
                    'msg': {
                        "type": 'string'
                    }
                }
            }
        }
    }
}

path_swag = {
    'summary': "Find path",
    'description': "Find path between 2 wiki articles",
    'consumes': "application/json",
    'produces': "application/json",
    'parameters': [
        {
            'name': "Authorization",
            'description': 'JWT token',
            'in': "header",
            'required': True,
            'schema': {
                'type': 'string'
            }
        },
        {
            'name': "A",
            'description': 'Start article',
            'in': "body",
            'required': True,
            'schema': {
                'type': 'string'
            }
        },
        {
            'name': "B",
            'description': 'End article',
            'in': "body",
            'required': True,
            'schema': {
                'type': 'string'
            }
        }
    ],
    'responses': {
        "200": {
            'description': "Path",
            'schema': {
                "type": "object",
                'properties': {
                    'msg': {
                        "type": 'string'
                    }
                }
            }
        },
        "400": {
            'description': "Error message",
            'schema': {
                "type": "object",
                'properties': {
                    'msg': {
                        "type": 'string'
                    }
                }
            }
        }
    }
}

login_swag = {
    'summary': "Generate authorization token",
    'description': "Generate authorization token",
    'consumes': "application/json",
    'produces': "application/json",
    'parameters': [
        {
            'name': "username",
            'in': "body",
            'required': True,
            'schema': {
                'type': 'string'
            }
        },
        {
            'name': "password",
            'in': "body",
            'required': True,
            'schema': {
                'type': 'string'
            }
        }
    ],
    'responses': {
        "200": {
            'description': "Auth token has been generated successfully",
            'schema': {
                "type": "object",
                'properties': {
                    'username': {
                        "type": 'string'
                    },
                    'token': {
                        "type": 'string'
                    }
                }
            }
        },
        '400': {
            'description': "Error message",
            'schema': {
                "type": "object",
                'properties': {
                    'msg': {
                        "type": 'string'
                    }
                }
            }
        }
    }
}

history_swag = {
    'summary': "Get history",
    'description': "Get requests history",
    'consumes': "application/json",
    'produces': "application/json",
    'parameters': [
        {
            'name': "Authorization",
            'description': 'JWT token',
            'in': "header",
            'required': True,
            'schema': {
                'type': 'string'
            }
        }
    ],
    'responses': {
        "200": {
            'description': "Returns an array of history records",
            'schema': {
                "type": "object",
                'properties': {
                    'history': {
                        'schema': {
                            "type": "array"
                        }
                    }
                }
            }
        }
    }
}

subscribe_swag = {
    'summary': "Subscribe to one of the possible plans",
    'description': "Subscribe to one of the possible plans",
    'consumes': "application/json",
    'produces': "application/json",
    'parameters': [
        {
            'name': "Authorization",
            'description': 'JWT token',
            'in': "header",
            'required': True,
            'schema': {
                'type': 'string'
            }
        },
        {
            'name': "subscription",
            'description': 'Subscription type',
            'in': "path",
            'required': True,
            'schema': {
                'enum': ['free', 'standard', 'pro']
            }
        }
    ],
    'responses': {
        "200": {
            'description': "Success",
            'schema': {
                "type": "object",
                'properties': {
                    'username': {
                        "type": 'string'
                    },
                    'subscription': {
                        "type": 'string'
                    }
                }
            }
        },
        '400': {
            'description': "Error message",
            'schema': {
                "type": "object",
                'properties': {
                    'msg': {
                        "type": 'string'
                    }
                }
            }
        }
    }
}
