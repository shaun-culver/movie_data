import click
import pandas as pd
import json
import asyncio
import os
import requests
from openai import OpenAI

api_key = ''
client = OpenAI(api_key=api_key)

@click.group()
def cli():
    """Fetches data using OpenAI and outputs it in various formats."""
    pass

@cli.command()
@click.option('--output', '-o', type=click.Choice(['json', 'csv', 'sql'], case_sensitive=False), default='json', help='Output format: json, csv, sql')
@click.option('--count', '-c', type=int, default=1, help='Number of listings to generate.')
@click.option('--directory', '-d', type=str, default='images', help='Directory to save the images.')
def fetch(output, count, directory):
    """Fetch data from OpenAI and output in specified format."""
    asyncio.run(fetch_async(output, count, directory))

async def fetch_async(output, count, directory):
    """Asynchronous function to fetch data and write it to the specified format."""
    try:
        data = fetch_data_from_openai(count)
        if output == 'json':
            await write_json(data, 'data.json')
        elif output == 'csv':
            await write_csv(data, 'data.csv')
        elif output == 'sql':
            await write_sql(data, 'data.sql')

        os.makedirs(directory, exist_ok=True)

        for listing in data:
            await generate_and_save_image(listing, directory)

        click.echo(f"Data successfully written to data.{output} and images saved to {directory}.")
    except Exception as e:
        click.echo(f"An error occurred: {e}", err=True)

def fetch_data_from_openai(count):
    """Fetches data from OpenAI and returns it as a list of dictionaries."""

    schema = {
        "type": "object",
        "properties": {
            "id": {
                "type": "string"
            },
            "title": {
                "type": "string"
            },
            "actors": {
                "type": "string"
            },
            "price": {
                "type": "string"
            }
        },
        "required": ["id", "title", "host", "price"]  # Optional: specify required fields
    }

    # Create a chat completion request
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are an assistant that generates structured data."},
            {"role": "user", "content": f"Please generate an array of {count} JSON objects for fictitional movies to see at a theatre."}
        ],
        temperature=0.7,
        functions=[
            {
                "name": "generate_listings",
                "description": "Generate multiple fictitional movie listings",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "listings": {
                            "type": "array",
                            "items": schema
                        }
                    }
                }
            }
        ],
        function_call={"name": "generate_listings"}
    )

    response_content = response.choices[0].message.function_call.arguments

    data = json.loads(response_content)
    return data['listings']

async def generate_and_save_image(listing, directory):
    """Generates an image for the listing and saves it to the specified directory."""
    prompt = f"A fun colorful fictitional movie poster for a movie titled '{listing['title']}'."

    image_response = client.images.generate(
        prompt=prompt,
        n=1,
        size="1024x1024"
    )

    image_url = image_response.data[0].url

    image_data = requests.get(image_url).content
    image_path = os.path.join(directory, f"{listing['id']}.png")
    with open(image_path, 'wb') as handler:
        handler.write(image_data)

async def write_json(data, filename):
    """Writes data to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

async def write_csv(data, filename):
    """Writes data to a CSV file."""
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

async def write_sql(data, filename):
    """Writes data to a SQL file."""
    with open(filename, 'w') as f:
        f.write("CREATE TABLE listings (id TEXT, title TEXT, host TEXT, price TEXT);\n")
        for item in data:
            f.write(f"INSERT INTO listings (id, title, host, price) VALUES ('{escape_sql(item.get('id', ''))}', '{escape_sql(item.get('title', ''))}', '{escape_sql(item.get('host', ''))}', '{escape_sql(item.get('price', ''))}');\n")

def escape_sql(value):
    """Escapes single quotes in SQL strings."""
    return value.replace("'", "''")

if __name__ == '__main__':
    cli()
