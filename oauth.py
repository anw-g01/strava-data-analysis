from stravalib import Client
from dotenv import load_dotenv
import os
import re 
import json
# suppress stravalib warnings:
import logging
os.environ["SILENCE_TOKEN_WARNINGS"] = "true"
logging.getLogger("stravalib").setLevel(logging.ERROR)


def _get_url(client_id: str, client_secret: str, redirect_uri: str) -> str:
    """Builds an OAuth 2.0 authorisation URL for an athlete to grant Strava app access."""
    
    client = Client()
    
    return client.authorization_url(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scope=["read", "read_all", "profile:read_all", "activity:read_all"],
    )


def _get_tokens(client_id: str, client_secret: str, view_tokens: bool = False) -> dict:
    """Exchanges an authorisation code for access and refresh tokens."""
    
    redirect_url = input("paste redirect URL: ")    # paste input for redirect URL to extract auth code
    auth_code = re.search(r"code=([^&]+)", redirect_url).group(1)   # regex pattern [^&]+ captures every char until "&":
    
    client = Client()

    # exchange auth code for a permanent refresh token (store securely):
    token_response = client.exchange_code_for_token(
        client_id=client_id,
        client_secret=client_secret,
        code=auth_code,
    )

    if view_tokens:
        _print_tokens(token_response)    
        
    return token_response


def _print_tokens(token_response: dict) -> None:
    print(f"\noutputting access and refresh tokens (DO NOT SHARE OR COMMIT):")
    print(f"{json.dumps(token_response, indent=4)}")


def authorise(view_tokens: bool = False) -> dict:
    """Runs the full OAuth 2.0 authorisation code flow for a single Strava athlete."""

    client_id, client_secret = os.getenv("CLIENT_ID"), os.getenv("CLIENT_SECRET")

    url = _get_url(client_id, client_secret, redirect_uri="http://localhost/exchange_token")
    print(f"\nClick URL: {url}\n")

    token_response = _get_tokens(client_id, client_secret, view_tokens)

    return token_response


def get_athlete(
    client_id: str, 
    client_secret: str, 
    refresh_token: str, 
    view_tokens: bool = False,
    verbose: bool = True
) -> Client:
    """Refreshes an access token using a refresh token and returns an authorised Strava client."""

    client = Client()

    # exchange refresh token for a new short lived access token:
    token_response = client.refresh_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token
    )

    if view_tokens:
        _print_tokens(token_response)  

    # re-initialise client with valid access token:
    client = Client(access_token=token_response["access_token"])

    # fetch athlete profile to verify successful authorisation:
    athlete = client.get_athlete()
    print(f"\nSuccessfully authenticated athlete: {athlete.firstname} {athlete.lastname}")

    # print profile details
    if verbose:
        profile_details = json.dumps(athlete.__dict__, indent=4, default=str)
        print(f"\n{profile_details}")

    return client


def main() -> None:

    load_dotenv()   # parses environment variables from .env file

    # run once to complete OAuth authorisation:
    # token_response = authorise(view_tokens=True)

    # refresh an access token, authenticate the athlete, and return an authorised client:
    client = get_athlete(
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"), 
        refresh_token=os.getenv("REFRESH_TOKEN1"),
        view_tokens=True,
        verbose=False
    )
    

if __name__ == "__main__":

    main()