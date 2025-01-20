import json

import looker_sdk
import tqdm
from looker_sdk import error, models40, serialize as sr
from slugify import slugify

sdk = looker_sdk.init40()  # or init31() for the older v3.1 API


def get_dashboard_list():
    dashboards_query = models40.WriteQuery(
        model="system__activity",
        view="dashboard",
        fields=[
            "dashboard.id",
            "dashboard.title",
            "dashboard_element.count",
            "dashboard_element.count_text",
            "query.count",
        ],
        pivots=None,
        fill_fields=None,
        filters={
            "dashboard.deleted_date": "NULL",
            "dashboard.moved_to_trash": "No",
        },
        filter_expression=None,
        sorts=["query.count desc"],
        limit="5000",
    )

    get_dashboards_query = sdk.create_query(body=dashboards_query)

    return json.loads(sdk.run_query(query_id=get_dashboards_query.id, result_format="json", cache=True))


def export_dashboards():
    dashboard_list = get_dashboard_list()

    count = 0
    for dashboard in tqdm.tqdm(dashboard_list):
        dashboard_id = str(dashboard["dashboard.id"])
        title = slugify(dashboard["dashboard.title"])
        try:
            dashboard_lookml = sdk.dashboard_lookml(dashboard_id=dashboard_id)['lookml']
            with open(f'dashboards/{dashboard_id}-{title}.dashboard.lookml', 'w') as fp:
                fp.write(dashboard_lookml)
        except error.SDKError:
            count += 1
            print(f"Broken dashboard, dashboard LookML was not imported for dashboard {dashboard_id}.")

    print(f"Total: {len(dashboard_list)} dashboards")
    print(f"Failed to extract {count} lookml")


def export_looks():
    looks = sdk.search_looks(deleted=False)
    count = 0
    for look in tqdm.tqdm(looks):
        try:
            full_look = sdk.look(look.id)
            title = slugify(full_look.title)
            with open(f'looks/{look.id}-{title}.json', 'wb') as fp:
                fp.write(sr.serialize(api_model=full_look, converter=full_look._get_converter()))
        except error.SDKError:
            count += 1
            print(f"Cannot extract details for look {look.id}.")

    print(f"Total: {len(looks)} dashboards")
    print(f"Failed to extract {count} looks")


if __name__ == '__main__':
    # Make sure that the environment variables defined in
    # https://github.com/looker-open-source/sdk-codegen?tab=readme-ov-file#environment-variable-configuration have been
    # set.
    export_dashboards()
    export_looks()
