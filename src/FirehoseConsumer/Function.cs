using Amazon;
using Amazon.KinesisFirehose;
using Amazon.Lambda.Core;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Newtonsoft.Json.Linq;
using System;
using System.Data.SqlClient;
using System.IO;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace FirehoseConsumer
{
    internal class Connnection
    {
        internal string username { get; set; }
        internal string password { get; set; }
        internal string engine { get; set; }
        internal string host { get; set; }
        internal string port { get; set; }
        internal string dbInstanceIdentifier { get; set; }
    }

    public class Function
    {
        /*
         * AWSSDK.SecretsManager version="3.3.0" targetFramework="net45"
         */

        public static string GetSecret(ILambdaContext context)
        {
            string secretName = "ReadPnlyWideWorlImporters"; // Environment.GetEnvironmentVariable("SecretName");
            string region = "us-east-2";
            string secret = "read_only"; // Environment.GetEnvironmentVariable("DBUser");

            MemoryStream memoryStream = new MemoryStream();

            IAmazonSecretsManager client = new AmazonSecretsManagerClient(RegionEndpoint.GetBySystemName(region));

            GetSecretValueRequest request = new GetSecretValueRequest();
            request.SecretId = secretName;
            request.VersionStage = "AWSCURRENT"; // VersionStage defaults to AWSCURRENT if unspecified.

            GetSecretValueResponse response = null;

            // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
            // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            // We rethrow the exception by default.
            context.Logger.Log("i am here");

            try
            {
                response = client.GetSecretValueAsync(request).Result;
                context.Logger.Log($"{response.ARN}");
            }
            catch (DecryptionFailureException e)
            {
                // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                // Deal with the exception here, and/or rethrow at your discretion.
                throw;
            }
            catch (InternalServiceErrorException e)
            {
                // An error occurred on the server side.
                // Deal with the exception here, and/or rethrow at your discretion.
                throw;
            }
            catch (InvalidParameterException e)
            {
                // You provided an invalid value for a parameter.
                // Deal with the exception here, and/or rethrow at your discretion
                throw;
            }
            catch (InvalidRequestException e)
            {
                // You provided a parameter value that is not valid for the current state of the resource.
                // Deal with the exception here, and/or rethrow at your discretion.
                throw;
            }
            catch (ResourceNotFoundException e)
            {
                // We can't find the resource that you asked for.
                // Deal with the exception here, and/or rethrow at your discretion.
                throw;
            }
            catch (System.AggregateException ae)
            {
                // More than one of the above exceptions were triggered.
                // Deal with the exception here, and/or rethrow at your discretion.
                throw;
            }

            // Decrypts secret using the associated KMS CMK.
            // Depending on whether the secret is a string or binary, one of these fields will be populated.
            var retVal = string.Empty;
            context.Logger.LogLine($"secret string {response.SecretString}");
            if (response.SecretString != null)
            {
                retVal = response.SecretString;
            }
            else
            {
                memoryStream = response.SecretBinary;
                StreamReader reader = new StreamReader(memoryStream);
                retVal = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(reader.ReadToEnd()));
                context.Logger.LogLine($"secret string {retVal}");
            }
            return retVal;

            // Your code goes here.
        }

        public void Connect(string rdsString, ILambdaContext context)
        {
            var connection = JObject.Parse(rdsString); //JsonConvert.DeserializeObject<Connnection>(rdsString);
            var pw = connection["password"];
            var un = connection["username"];
            var url = connection["host"];
            var port = connection["port"];
            var dbName = connection["dbInstanceIdentifier"];
            var cxnString = $"DataSource={url};username={un};Password={pw}";
            var cxnString1 = $"Data Source={url}, {port}; Initial Catalog={dbName}; User ID={un}; Password={pw};";
            context.Logger.Log(cxnString1);
            using (SqlConnection con = new SqlConnection(cxnString1))
            {
                string query = "select top 10 * from [Application].[Cities]";
                SqlCommand cmd = new SqlCommand(query, con);
                con.Open();
                SqlDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    context.Logger.Log(reader.GetString(reader.GetOrdinal("CityName")));
                }
            }
        }

        public string PushToFirehose(string stuffToPush)
        {
            var retVal = string.Empty;

            var config = new AmazonKinesisFirehoseConfig();
            config.RegionEndpoint = RegionEndpoint.USEast2;
            config.ServiceURL = string.Empty;

            var client = new AmazonKinesisFirehoseClient(config);

            return retVal;
        }

        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public string FunctionHandler(ILambdaContext context)
        {
            var secret = GetSecret(context);
            context.Logger.Log($"secret string {secret}");
            context.Logger.Log("before connect");
            Connect(secret, context);
            return secret;
        }
    }
}