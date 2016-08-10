using System;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace Gw.Persistence.DapperHelpers
{
    public class SqlClientHelper
    {
        private static string defaultConnectionString = null;

        public static void SetDefaultConnectionString(string connectionString)
        {
            defaultConnectionString = connectionString;
        }

        public static string DefaultConnectionString => defaultConnectionString;
        public static SqlConnection GetSqlConnection(string connectionString = null)
        {
            if(string.IsNullOrEmpty(connectionString ?? defaultConnectionString))
                throw new Exception("You must define a default sql connection string if you don't provide one in this call: SqlClientHelper.SetDefaultConnectionString(conn)");

            return new SqlConnection(connectionString ?? defaultConnectionString);
        }

        public static Result<T> WithOpenSqlConnection<T>(Func<SqlConnection, T> action)
        {
            try
            {
                using (var connection = GetSqlConnection())
                {
                    connection.Open();

                    T result = action(connection);
                    return Result<T>.FromSuccess(result);
                }
            }
            catch (Exception ex)
            {
                return Result<T>.FromFailure(ex);
            }
        }

        public static async Task<Result<T>> WithOpenSqlConnectionAsync<T>(Func<SqlConnection, Task<T>> sqlQuery, string connectionstring = null)
        {
            try
            {
                using (var connection = GetSqlConnection(connectionstring))
                {
                    connection.Open();

                    T result = await sqlQuery(connection);
                    return Result<T>.FromSuccess(result);
                }
            }
            catch (Exception ex)
            {
                return Result<T>.FromFailure(ex.Message);
            }
        }
    }
}
