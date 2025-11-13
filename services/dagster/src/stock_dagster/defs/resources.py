from functools import cached_property
import dagster as dg
from dagster import ConfigurableResource
from data_access import DuckS3
from google import genai


class DuckDBS3Resource(ConfigurableResource):
    bucket_name: str

    def get_resource(self) -> DuckS3:
        return DuckS3(self.bucket_name)


class GeminiResource(ConfigurableResource):
    api_key: str
    model_name: str = 'gemini-2.5-flash'

    @cached_property
    def client(self) -> genai.Client:
        return genai.Client(api_key=self.api_key)

    def send_request(self, prompt: str) -> str:
        response = self.client.models.generate_content(
            contents=prompt,
            model=self.model_name
        )
        return response.text


ducks3 = DuckDBS3Resource(bucket_name=dg.EnvVar('S3_BUCKET'))
gemini = GeminiResource(api_key=dg.EnvVar('GEMINI_API_KEY'))

defs = dg.Definitions(
    resources={
        'ducks3': ducks3,
        'gemini': gemini
    }
)
