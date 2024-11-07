import logging
import json
import requests
import pandas as pd
import gspread
import gspread_dataframe as gd
from time import sleep
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
import tempfile
from airflow.exceptions import AirflowFailException
import os

VARIABLE_SUFFIX = "shopify_reporting"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=20),
}

dag = DAG(
    'Shopify_To_GS_V1.02',
    default_args=default_args,
    description='DAG for fetching data from Shopify and uploading it to Google Sheets',
    schedule_interval='35 16 * * *',
)

stores = {
    "SHOPIFY_CARPORT": ("orders_shopify_nanocarport", "customers_shopify_nanocarport"),
    "SHOPIFY_EU": ("orders_shopify_EU", "customers_shopify_EU"),
}

def load_shopify_credentials(store):
    shop_name = Variable.get(f"{VARIABLE_SUFFIX}_{store}_shop_name")
    access_token = Variable.get(f"{VARIABLE_SUFFIX}_{store}_API_ACCESS_TOKEN")
    api_version = Variable.get(f"{VARIABLE_SUFFIX}_API_VERSION", "2024-01")
    return shop_name, access_token, api_version

def flatten_order(order):
    # Extract the basic fields from the order
    flat_order = {
        'order_id': order['id'],
        'cancel_reason': order['cancelReason'],
        'cancelled_at': order['cancelledAt'],
        'updated_at': order['updatedAt'],
        'created_at': order['createdAt'],
        'unpaid': order['unpaid'],
        'fully_paid': order['fullyPaid'],
        'displayFulfillmentStatus': order['displayFulfillmentStatus'],
        'total_price_amount': order['totalPriceSet']['presentmentMoney']['amount'],
        'total_price_currency': order['totalPriceSet']['presentmentMoney']['currencyCode'],
        'total_price_shop_amount': order['totalPriceSet']['shopMoney']['amount'],
        'total_price_shop_currency': order['totalPriceSet']['shopMoney']['currencyCode'],
        'billing_city': order['billingAddress']['city'] if order['billingAddress'] else None,
        'billing_country': order['billingAddress']['country'] if order['billingAddress'] else None,
        'billing_country_code': order['billingAddress']['countryCodeV2'] if order['billingAddress'] else None,
        'display_city': order['displayAddress']['city'] if order['displayAddress'] else None,
        'display_country': order['displayAddress']['country'] if order['displayAddress'] else None,
        'display_country_code': order['displayAddress']['countryCodeV2'] if order['displayAddress'] else None,
        'customer_id': order['customer']['id'] if order['customer'] else None,
        'customer_email': order['customer']['email'] if order['customer'] else None,
        'customer_first_name': order['customer']['firstName'] if order['customer'] else None,
        'customer_last_name': order['customer']['lastName'] if order['customer'] else None,
        'customer_tags': ','.join(order['customer']['tags']) if order['customer'] and order['customer']['tags'] else None,
        'email_marketing_consent_updated_at': order['customer']['emailMarketingConsent']['consentUpdatedAt'] if order['customer'] and order['customer']['emailMarketingConsent'] else None,
        'email_marketing_opt_in_level': order['customer']['emailMarketingConsent']['marketingOptInLevel'] if order['customer'] and order['customer']['emailMarketingConsent'] else None,
        'email_marketing_state': order['customer']['emailMarketingConsent']['marketingState'] if order['customer'] and order['customer']['emailMarketingConsent'] else None,
    }

    # Process transactions
    transactions = []
    for transaction in order.get('transactions', []):
        transaction_data = {
            'transaction_id': transaction['id'],
            'transaction_created_at': transaction['createdAt'],
            'transaction_kind': transaction['kind'],
            'transaction_status': transaction['status']
        }
        transactions.append(transaction_data)

    # Process metafields if they exist
    metafields = {}
    if order['customer'] and 'metafields' in order['customer']:
        for metafield_edge in order['customer']['metafields']['edges']:
            metafield = metafield_edge['node']
            metafields[metafield['key']] = metafield['value']

    flat_order.update(metafields)  # Add metafields to the flat order

    # Extract line items (flatten each item and append it to the flat_order dict)
    line_items = []
    for line_item_edge in order['lineItems']['edges']:
        line_item = line_item_edge['node']
        flat_item = {
            'line_item_id': line_item['id'],
            'line_item_name': line_item['name'],
            'line_item_quantity': line_item['quantity'],
            'line_item_sku': line_item['sku'],
            'line_item_price_amount': line_item['originalUnitPriceSet']['presentmentMoney']['amount'],
            'line_item_price_currency': line_item['originalUnitPriceSet']['presentmentMoney']['currencyCode'],
            'line_item_price_shop_amount': line_item['originalUnitPriceSet']['shopMoney']['amount'],
            'line_item_price_shop_currency': line_item['originalUnitPriceSet']['shopMoney']['currencyCode'],
        }
        # Append each flattened line item to the list
        line_items.append({**flat_order, **flat_item})

    # Include transaction data if available
    for transaction in transactions:
        for line_item in line_items:
            line_item.update(transaction)

    return line_items

def flatten_customers(customer):
    # Extract basic fields from the customer
    flat_customer = {
        'customer_id': customer['id'],
        'customer_email': customer['email'],
        'customer_first_name': customer['firstName'],
        'customer_last_name': customer['lastName'],
        'customer_tags': ','.join(customer['tags']) if customer['tags'] else None,
        'created_at': customer['createdAt'],
        'updated_at': customer['updatedAt'],
        'email_marketing_consent_updated_at': customer['emailMarketingConsent']['consentUpdatedAt'] if customer.get('emailMarketingConsent') else None,
        'email_marketing_opt_in_level': customer['emailMarketingConsent']['marketingOptInLevel'] if customer.get('emailMarketingConsent') else None,
        'email_marketing_state': customer['emailMarketingConsent']['marketingState'] if customer.get('emailMarketingConsent') else None,
    }

    # Process addresses if they exist
    addresses = []
    for address in customer.get('addresses', []):
        flat_address = {
            'address_city': address['city'],
            'address_country': address['country'],
            'address_country_code': address['countryCodeV2'],
        }
        addresses.append(flat_address)
        # Process metafields if they exist
    metafields = {}
    if customer['metafields']:
        for metafield_edge in  customer['metafields']['edges']:
            metafield = metafield_edge['node']
            metafields[metafield['key']] = metafield['value']

    flat_customer.update(metafields)  # Add metafields to the flat order
    # If multiple addresses, combine with the customer data
    if addresses:
        for address in addresses:
            combined_data = {**flat_customer, **address}
            yield combined_data  # Use yield for memory efficiency

    else:
        yield flat_customer  # If no addresses, yield just the flat customer data

def fetch_orders(store, **context):
    shop_name, access_token, api_version = load_shopify_credentials(store)
    url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    
    query = '''
    {
      orders(first: 250) {
        edges {
          node {
            id
            cancelReason
            cancelledAt
            updatedAt
            createdAt
            unpaid
            fullyPaid
            displayFulfillmentStatus
            totalPriceSet {
              presentmentMoney {
                amount
                currencyCode
              }
              shopMoney {
                amount
                currencyCode
              }
            }
            billingAddress {
              city
              country
              countryCodeV2
            }
            displayAddress {
              city
              country
              countryCodeV2
            }
            lineItems(first: 100) {
              edges {
                node {
                  id
                  name
                  quantity
                  sku
                  originalUnitPriceSet {
                    presentmentMoney {
                      amount
                      currencyCode
                    }
                    shopMoney {
                      amount
                      currencyCode
                    }
                  }
                }
              }
            }
            transactions {
              id
              createdAt
              kind
              status
            }
            customer {
              id
              email
              firstName
              lastName
              tags
              emailMarketingConsent {
                consentUpdatedAt
                marketingOptInLevel
                marketingState
              }
              metafields(first: 100) {
                edges {
                  node {
                    key
                    value
                  }
                }
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    '''

    flat_orders = []
    has_next_page = True
    end_cursor = None

    while has_next_page:
        # Modify query if pagination is needed
        paginated_query = query
        if end_cursor:
            paginated_query = query.replace('}', f', after: "{end_cursor}"}}')

        try:
            response = requests.post(url, headers=headers, json={'query': paginated_query})

            if response.status_code == 200:
                response_data = response.json()
                # Check if 'data' key exists
                if 'data' not in response_data or 'orders' not in response_data['data']:
                    logging.error(f"Unexpected response structure: {response_data}")
                    raise AirflowFailException("No 'data' key found in response.")

                orders_edges = response_data['data']['orders']['edges']

                for edge in orders_edges:
                    flat_orders.extend(flatten_order(edge['node']))

                page_info = response_data['data']['orders']['pageInfo']
                has_next_page = page_info['hasNextPage']
                end_cursor = page_info.get('endCursor')

                sleep(1)  # Avoid hitting rate limits
            else:
                logging.error(f"Failed to fetch orders. Status code: {response.status_code}, Response: {response.text}")
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            raise AirflowFailException(f"Request to Shopify API failed: {e}")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
        temp_file.write(json.dumps(flat_orders).encode('utf-8'))
        temp_file.flush()
        context['ti'].xcom_push(key=f"{store}_orders_file_path", value=temp_file.name)

    return

def fetch_customers(store, **context):
    shop_name, access_token, api_version = load_shopify_credentials(store)
    url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }
    query = '''
    {
      customers(first: 250) {
        edges { node { id email firstName lastName tags createdAt updatedAt addresses(first: 100) { city country countryCodeV2 }
          emailMarketingConsent { consentUpdatedAt marketingOptInLevel marketingState } metafields(first: 100) { edges { node { key value }}}}}
        pageInfo { hasNextPage endCursor }}}
    '''

    flat_customers = []
    has_next_page = True
    end_cursor = None

    while has_next_page:
        paginated_query = query if not end_cursor else query.replace('}', f', after: "{end_cursor}"}}')
        response = requests.post(url, headers=headers, json={'query': paginated_query})

        if response.status_code == 200:
            data = response.json()['data']['customers']

            customers_edges = response.json()['data']['customers']['edges']

            for edge in customers_edges:
                flat_customers.extend(flatten_customers(edge['node']))

            page_info = data['pageInfo']
            has_next_page = page_info['hasNextPage']
            end_cursor = page_info.get('endCursor')
            sleep(1)
        else:
            logging.error(f"Failed to fetch customers: {response.content}")
            response.raise_for_status()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
        temp_file.write(json.dumps(flat_customers).encode('utf-8'))
        temp_file.flush()
        context['ti'].xcom_push(key=f"{store}_customers_file_path", value=temp_file.name)

def save_to_google_sheets(stores, **context):
    gsheet_credentials_json = Variable.get("gsheet_credentials_1")
    gsheet_name = Variable.get(f"{VARIABLE_SUFFIX}_spreadsheet")

    with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
        tmp_file.write(gsheet_credentials_json.encode('utf-8'))
        tmp_file.flush()
        gc = gspread.service_account(filename=tmp_file.name)

        for store, (tab_orders, tab_customers) in stores.items():
            # Retrieve paths from XCom
            orders_path = context['ti'].xcom_pull(key=f"{store}_orders_file_path")
            customers_path = context['ti'].xcom_pull(key=f"{store}_customers_file_path")

            # Validate paths
            if not orders_path or not customers_path:
                raise AirflowFailException(f"File path(s) for {store} are missing. Check XCom push/pull consistency.")
            
            # Read flattened JSON files into DataFrames
            orders_df = pd.read_json(orders_path)
            customers_df = pd.read_json(customers_path)

            sheet = gc.open(gsheet_name)

            # Orders worksheet
            try:
                orders_worksheet = sheet.worksheet(tab_orders)
                orders_worksheet.clear()
            except gspread.exceptions.WorksheetNotFound:
                orders_worksheet = sheet.add_worksheet(title=tab_orders, rows="100", cols="20")

            # Customers worksheet
            try:
                customers_worksheet = sheet.worksheet(tab_customers)
                customers_worksheet.clear()
            except gspread.exceptions.WorksheetNotFound:
                customers_worksheet = sheet.add_worksheet(title=tab_customers, rows="100", cols="20")

            # Write orders data to Google Sheets
            gd.set_with_dataframe(orders_worksheet, orders_df)

            # Write customers data to Google Sheets
            gd.set_with_dataframe(customers_worksheet, customers_df)


def cleanup_temp_files(store, **context):
    orders_path = context['ti'].xcom_pull(key=f"{store}_orders_file_path")
    customers_path = context['ti'].xcom_pull(key=f"{store}_customers_file_path")

    # Attempt to delete the files if paths are valid
    if orders_path and os.path.exists(orders_path):
        os.remove(orders_path)
    if customers_path and os.path.exists(customers_path):
        os.remove(customers_path)

for store, (tab_orders, tab_customers) in stores.items():
    fetch_orders_task = PythonOperator(
        task_id=f'fetch_orders_{store}',
        python_callable=fetch_orders,
        op_kwargs={'store': store},
        dag=dag,
    )

    fetch_customers_task = PythonOperator(
        task_id=f'fetch_customers_{store}',
        python_callable=fetch_customers,
        op_kwargs={'store': store},
        dag=dag,
    )

    save_to_google_sheets_task = PythonOperator(
        task_id=f'save_to_google_sheets_{store}',
        python_callable=save_to_google_sheets,
        op_kwargs={'stores': stores},
        provide_context=True,
        dag=dag,
    )
    cleanup_task = PythonOperator(
            task_id=f'cleanup_temp_files_{store}',
            python_callable=cleanup_temp_files,
            op_kwargs={'store': store},
            provide_context=True,
            dag=dag,
        )

    # Set task dependencies
    [fetch_orders_task, fetch_customers_task] >> save_to_google_sheets_task >> cleanup_task