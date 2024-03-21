package treestore

const indexJson = `{
"data": {
	"account-numbers": {
	"0": null,
	"test@nab.com": null
	},
	"accounts": {
	"2ff301d7-1fc1-4aad-9cf1-19106240059b": {
		"excluded_mid_pattern": [],
		"included_mid_pattern": [
		"*"
		],
		"last_access_iso": "2023-10-12T09:23:06-04:00",
		"number": "test@nab.com",
		"owner_user_ids": [
		"370f1600-5153-435c-b1e9-5c98bd3f2cf9"
		],
		"portfolio_ids": [
		"4f838cfc-d04f-4c54-8f34-caf75360d651",
		"f435efad-7fe5-471f-b927-76ff82c64d17"
		]
	},
	"fff31441-747f-4e64-9549-4bcfa7f6e315": {
		"excluded_mid_pattern": [],
		"included_mid_pattern": [
		"*"
		],
		"last_access_iso": "2023-10-12T09:23:06-04:00",
		"number": "0",
		"owner_user_ids": [
		"59250cef-7f5b-43d3-8079-d23f1da0f238"
		],
		"portfolio_ids": [
		"673e9932-bd0a-432d-b672-b8fbd7a6db10"
		]
	}
	},
	"cases": null,
	"organization-names": {
	":global": null,
	"test@nab.com": null
	},
	"organizations": {
	"7af47a45-8d87-4398-8612-e0d8d72562ed": {
		"account_ids": [
		"fff31441-747f-4e64-9549-4bcfa7f6e315"
		],
		"disabled_until": 0,
		"name": ":global",
		"parent_org_id": ""
	},
	"954a041d-121d-46c9-a319-0fd2eaae7e43": {
		"account_ids": [
		"2ff301d7-1fc1-4aad-9cf1-19106240059b"
		],
		"disabled_until": 0,
		"name": "test@nab.com",
		"parent_org_id": ""
	}
	},
	"portfolios": {
	"4f838cfc-d04f-4c54-8f34-caf75360d651": {
		"account_id": "2ff301d7-1fc1-4aad-9cf1-19106240059b",
		"built_in": true,
		"last_access_iso": "2023-10-12T09:23:10-04:00",
		"name": "Entire Account",
		"selected_mid_patterns": [
		"*"
		]
	},
	"673e9932-bd0a-432d-b672-b8fbd7a6db10": {
		"account_id": "fff31441-747f-4e64-9549-4bcfa7f6e315",
		"built_in": true,
		"last_access_iso": "2023-10-12T09:23:06-04:00",
		"name": "Entire Account",
		"selected_mid_patterns": [
		"*"
		]
	},
	"f435efad-7fe5-471f-b927-76ff82c64d17": {
		"account_id": "2ff301d7-1fc1-4aad-9cf1-19106240059b",
		"built_in": false,
		"last_access_iso": "2023-10-12T09:23:44-04:00",
		"name": "Testing",
		"selected_mid_patterns": [
		"3001-*"
		]
	}
	},
	"portfolios-account-name": {
	"2ff301d7-1fc1-4aad-9cf1-19106240059b": {
		"Entire Account": null,
		"Testing": null
	},
	"fff31441-747f-4e64-9549-4bcfa7f6e315": {
		"Entire Account": null
	}
	},
	"roles": {
	"14acf162-924a-4165-b5d6-fa637dd12a8c": {
		"assign_rights": {},
		"context": "custom",
		"hidden": false,
		"name": "Tester",
		"org_id": "954a041d-121d-46c9-a319-0fd2eaae7e43",
		"rights": {}
	},
	"68812e0b-2efd-4685-8064-6e045ead12ee": {
		"assign_rights": {},
		"context": "global",
		"hidden": true,
		"name": "Case Manager",
		"org_id": ":global",
		"rights": {
		"casemgr": 15
		}
	},
	"945792ee-2ad8-4f2f-99f4-45cc296e04c5": {
		"assign_rights": {
		"casemgr": 4294967295,
		"usermgr": 4294967295
		},
		"context": "global",
		"hidden": false,
		"name": "Owner",
		"org_id": ":global",
		"rights": {
		"casemgr": 4294967295,
		"usermgr": 4294967295
		}
	},
	"9dd778b1-503e-49d1-85b3-2fe8d9df450c": {
		"assign_rights": {},
		"context": "global",
		"hidden": true,
		"name": "System",
		"org_id": ":global",
		"rights": {
		"usermgr": 96
		}
	},
	"a321caa8-453b-4ce2-8244-fd155547bda9": {
		"assign_rights": {
		"casemgr": 4294967295,
		"usermgr": 4294967295
		},
		"context": "global",
		"hidden": true,
		"name": "Root",
		"org_id": ":global",
		"rights": {
		"casemgr": 4294967295,
		"usermgr": 4294967295
		}
	}
	},
	"roles-name-index": {
	"954a041d-121d-46c9-a319-0fd2eaae7e43": {
		"custom": {
		"Tester": null
		}
	},
	":global": {
		"global": {
		"Case Manager": null,
		"Owner": null,
		"Root": null,
		"System": null
		}
	}
	},
	"roles-org-name-index": {
	"954a041d-121d-46c9-a319-0fd2eaae7e43": {
		"Tester": null
	},
	":global": {
		"Case Manager": null,
		"Owner": null,
		"Root": null,
		"System": null
	}
	},
	"staging": {
	"28919690-1e5c-4408-ac34-4a940bf66686": null,
	"370f1600-5153-435c-b1e9-5c98bd3f2cf9": null,
	"59250cef-7f5b-43d3-8079-d23f1da0f238": null,
	"a941a019-aed3-45d8-aefc-cead8e75df18": null
	},
	"users": {
	"email-org": {
		"case-loader@service.local": {
		"7af47a45-8d87-4398-8612-e0d8d72562ed": null
		},
		"sample@gmail.com": {
		"954a041d-121d-46c9-a319-0fd2eaae7e43": null
		},
		"system": {
		"7af47a45-8d87-4398-8612-e0d8d72562ed": null
		},
		"test@nab.com": {
		"954a041d-121d-46c9-a319-0fd2eaae7e43": null
		}
	},
	"org": {
		"7af47a45-8d87-4398-8612-e0d8d72562ed": {
		"28919690-1e5c-4408-ac34-4a940bf66686": null,
		"59250cef-7f5b-43d3-8079-d23f1da0f238": null
		},
		"954a041d-121d-46c9-a319-0fd2eaae7e43": {
		"370f1600-5153-435c-b1e9-5c98bd3f2cf9": null,
		"a941a019-aed3-45d8-aefc-cead8e75df18": null
		}
	},
	"profiles": {
		"28919690-1e5c-4408-ac34-4a940bf66686": {
		"change_password": false,
		"created_date": 1697116986,
		"email": "case-loader@service.local",
		"impersonate_user_ids": [],
		"login_portfolio": "673e9932-bd0a-432d-b672-b8fbd7a6db10",
		"name": "case-loader",
		"organization_id": "7af47a45-8d87-4398-8612-e0d8d72562ed",
		"password_hash": "JDJhJDA2JFpuL0RpZURTajA0UmxJUFpuTzkzLy4zdVhNY0JpZzNHZ2xpT2NHZER2bTdIQkxOekNZUHVP",
		"permissions": [
			{
			"portfolio_id": "673e9932-bd0a-432d-b672-b8fbd7a6db10",
			"role_id": "68812e0b-2efd-4685-8064-6e045ead12ee"
			}
		],
		"phone_number": "",
		"salt": "UxuopFCWcaUeCfuIA24bMA==",
		"stats": {
			"bad_attempts": 0,
			"login_date": 0
		},
		"suspended_until_date": 0,
		"verified": false
		},
		"370f1600-5153-435c-b1e9-5c98bd3f2cf9": {
		"change_password": false,
		"created_date": 1697116986,
		"email": "test@nab.com",
		"impersonate_user_ids": [],
		"login_portfolio": "4f838cfc-d04f-4c54-8f34-caf75360d651",
		"name": "Test User",
		"organization_id": "954a041d-121d-46c9-a319-0fd2eaae7e43",
		"password_hash": "JDJhJDA2JEhPT0p4WTFmQWVqUVJpQ0VQZ2s0aU8wN09MZ3dkaU4zSExQMlBIaUgzLlFsYk5NZTkuWmZX",
		"permissions": [
			{
			"portfolio_id": "4f838cfc-d04f-4c54-8f34-caf75360d651",
			"role_id": "945792ee-2ad8-4f2f-99f4-45cc296e04c5"
			}
		],
		"phone_number": "+01-313-555-1212",
		"salt": "zAWUnG01If/dNoQw/sSIzw==",
		"stats": {
			"bad_attempts": 0,
			"history": {
			"login": "1697116990"
			},
			"last_login": 1697116990,
			"login_date": 0
		},
		"suspended_until_date": 0,
		"verified": false
		},
		"59250cef-7f5b-43d3-8079-d23f1da0f238": {
		"change_password": false,
		"created_date": 1697116986,
		"email": "system",
		"impersonate_user_ids": [],
		"login_portfolio": "673e9932-bd0a-432d-b672-b8fbd7a6db10",
		"name": "System",
		"organization_id": "7af47a45-8d87-4398-8612-e0d8d72562ed",
		"password_hash": "JDJhJDA2JFN3YjNXaElJUGdkNDhnRi5HZGVhVnVEb2E3T0tHZy9MSUt2elVPZ01MaEJEekxDZE11L1Ft",
		"permissions": [
			{
			"portfolio_id": "673e9932-bd0a-432d-b672-b8fbd7a6db10",
			"role_id": "a321caa8-453b-4ce2-8244-fd155547bda9"
			}
		],
		"phone_number": "",
		"salt": "BnZmK6uRCF0WY/L3oSfNcg==",
		"stats": {
			"bad_attempts": 0,
			"login_date": 0
		},
		"suspended_until_date": 0,
		"verified": false
		},
		"a941a019-aed3-45d8-aefc-cead8e75df18": {
		"change_password": false,
		"created_date": 1697117012,
		"email": "sample@gmail.com",
		"impersonate_user_ids": [],
		"login_portfolio": "4f838cfc-d04f-4c54-8f34-caf75360d651",
		"name": "Testy",
		"organization_id": "954a041d-121d-46c9-a319-0fd2eaae7e43",
		"password_hash": "JDJhJDA2JGQ0bkVTVHBONk1kL1hBOTYyaHh4ZXViMTR4NXFOdk1pZE1TVGR3VlNtamN0RDVOYVlJTGwu",
		"permissions": [
			{
			"portfolio_id": "4f838cfc-d04f-4c54-8f34-caf75360d651",
			"role_id": "945792ee-2ad8-4f2f-99f4-45cc296e04c5"
			}
		],
		"phone_number": "",
		"salt": "ZXnFbX8iHah+eJU84eiRiA==",
		"stats": {
			"bad_attempts": 0,
			"login_date": 0
		},
		"suspended_until_date": 0,
		"verified": false
		}
	}
	}
}
}`

var _ = indexJson