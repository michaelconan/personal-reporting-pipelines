# General imports
import pendulum


# Sample page data from Notion API
PAGE = {
    "object": "page",
    "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
    "created_time": "2022-03-01T19:05:00.000Z",
    "last_edited_time": "2022-07-06T20:25:00.000Z",
    "created_by": {"object": "user", "id": "ee5f0f84-409a-440f-983a-a5315961c6e4"},
    "last_edited_by": {
        "object": "user",
        "id": "0c3e9826-b8f7-4f73-927d-2caaf86f1103",
    },
    "parent": {
        "type": "database_id",
        "database_id": "d9824bdc-8445-4327-be8b-5b47500af6ce",
    },
    "archived": False,
    "properties": {
        "Store availability": {
            "id": "%3AUPp",
            "type": "multi_select",
            "multi_select": [
                {"id": "t|O@", "name": "Gus's Community Market", "color": "yellow"},
                {"id": "{Ml\\", "name": "Rainbow Grocery", "color": "gray"},
            ],
        },
        "Food group": {
            "id": "A%40Hk",
            "type": "select",
            "select": {
                "id": "5e8e7e8f-432e-4d8a-8166-1821e10225fc",
                "name": "🥬 Vegetable",
                "color": "pink",
            },
        },
        "Price": {"id": "BJXS", "type": "number", "number": 2.5},
        "Last ordered": {
            "id": "Jsfb",
            "type": "date",
            "date": {"start": "2022-02-22", "end": None, "time_zone": None},
        },
        "Cost of next trip": {
            "id": "WOd%3B",
            "type": "formula",
            "formula": {"type": "number", "number": 0},
        },
        "Recipes": {
            "id": "YfIu",
            "type": "relation",
            "relation": [
                {"id": "90eeeed8-2cdd-4af4-9cc1-3d24aff5f63c"},
                {"id": "a2da43ee-d43c-4285-8ae2-6d811f12629a"},
            ],
            "has_more": False,
        },
        "Name": {
            "id": "title",
            "type": "title",
            "title": [
                {
                    "type": "text",
                    "text": {"content": "Tuscan kale", "link": None},
                    "annotations": {
                        "bold": False,
                        "italic": False,
                        "strikethrough": False,
                        "underline": False,
                        "code": False,
                        "color": "default",
                    },
                    "plain_text": "Tuscan kale",
                    "href": None,
                }
            ],
        },
    },
    "url": "https://www.notion.so/Tuscan-kale-598337872cf94fdf8782e53db20768a5",
}


def test_notion_filters():

    from pipelines.common.utils import filter_fields
    from pipelines.notion import EXCLUDE_PATHS

    # GIVEN
    # Use exclude paths

    # WHEN
    filtered = filter_fields(PAGE, EXCLUDE_PATHS)

    # THEN
    # Validate the filtered page contains expected properties
    print(filtered)


def test_notion_load():

    # Delayed import to capture DBT target variable
    from pipelines.notion import refresh_notion

    # GIVEN

    # WHEN
    # Run the pipeline
    refresh_notion(is_incremental=False)

    # THEN
    # Validate task instances were successful


def test_hubspot_load():

    # Delayed import to capture DBT target variable
    from pipelines.hubspot import refresh_hubspot

    # GIVEN

    # WHEN
    # Run the DAG
    info = refresh_hubspot(is_incremental=False)

    # THEN
    print(info)


def test_fitbit_load():

    # Delayed import to capture DBT target variable
    from pipelines.fitbit import refresh_fitbit

    # GIVEN

    # WHEN
    # Run the DAG
    info = refresh_fitbit(is_incremental=False)

    # THEN
    print(info)
