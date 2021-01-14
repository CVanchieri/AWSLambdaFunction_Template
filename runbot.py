import dotenv
from src.lambda_function import lambda_handler


### runs the bot locally ###
dotenv.load_dotenv()
if __name__ == "__main__":
    lambda_handler(event=None, context=None)

   
