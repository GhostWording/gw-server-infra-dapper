using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Gw.Utils;

namespace Gw.Persistence.DapperHelpers
{
    public class SqlTasksHelper
    {
        private string connectionString;
        public bool TraceEnabled { get; set; } = false;

        public SqlTasksHelper(string connectionString)
        {
            this.connectionString = connectionString;
        }
        private async Task<Result<int>> executeInternal(
            string query,
            object queryParams,
            int? commandTimeout = null
            ) => await SqlClientHelper.WithOpenSqlConnectionAsync(
                async conn => await Task.Run(() => conn.Execute(query, queryParams, commandTimeout: commandTimeout)),
                this.connectionString);

        public async Task<Result<int>> Execute(string query, object queryParams, int? commandTimeout = null)
        {
            if (TraceEnabled)
            {
                await AppLog.Info($"start executing sql [{query}]");
                var result = await executeInternal(query, queryParams, commandTimeout);
                await AppLog.Info($"end executing sql [{query}]");
                return result;
            }
            return await executeInternal(query, queryParams, commandTimeout);
        }

        public async Task<Result<IEnumerable<T>>> Query<T>(
            string query,
            object queryParams = null,
            int? commandTimeout = null
            ) => await SqlClientHelper.WithOpenSqlConnectionAsync(
                async conn => await conn.QueryAsync<T>(query, queryParams, commandTimeout: commandTimeout),
                this.connectionString);

        public async Task<Result<IEnumerable<TU>>> QueryMap<T, TU>
            (string query, object queryParams, Func<T, TU> actionForOne, int? commandTimeout = null)
            => await SqlClientHelper.WithOpenSqlConnectionAsync<IEnumerable<TU>>(
                async conn => await conn
                    .QueryAsync<T>(query, queryParams, commandTimeout: commandTimeout)
                    .ContinueWith(previous => previous.Result.Select(actionForOne)
                    ), this.connectionString);


        public async Task<Result<int>> BatchInsert<T>(
            string insertTemplate,
            IEnumerable<T> values,
            string beforeSql = null,
            string postSql = null,
            int? commandTimeout = null,
            bool useFullTransaction = false)
            => await SqlClientHelper.WithOpenSqlConnectionAsync(
                conn => Task.Run<int>( async () =>
                {
                    if (useFullTransaction)
                    {
                        await AppLog.Info($"start transaction");
                        SqlTransaction trans = conn.BeginTransaction();
                        if (!string.IsNullOrEmpty(beforeSql))
                        {
                            int prepareResult = conn.Execute(beforeSql, transaction: trans, commandTimeout: commandTimeout);
                            await AppLog.Info($"end pre-sql with result {prepareResult}");
                        }
                        int result = 0;
                        int counter = 0;
                        int take = 10000;
                        List<T> buffer = new List<T>();
                        foreach (var value in values)
                        {
                            buffer.Add(value);
                            if (buffer.Count == take)
                            {
                                try
                                {
                                    result += conn.Execute(insertTemplate, buffer,transaction:trans);
                                    counter += buffer.Count;
                                    buffer = new List<T>();
                                }
                                catch (Exception ex)
                                {
                                    buffer = new List<T>();
                                    await AppLog.Error($"error inserting line [{counter}] of batch : {ex.Message}");
                                }
                            }
                        }
                        if (buffer.Any())
                        {
                            try
                            {
                                result += conn.Execute(insertTemplate, buffer,transaction:trans);
                                counter += buffer.Count;
                                buffer = new List<T>();
                            }
                            catch (Exception ex)
                            {
                                await AppLog.Error($"error inserting line [{counter}] of batch : {ex.Message}");
                            }
                        }

                        await AppLog.Info($"end sql batch command, [{result}] lines inserted");
                        if (!string.IsNullOrEmpty(postSql))
                        {
                            int postResult = conn.Execute(postSql, transaction: trans, commandTimeout: commandTimeout);
                            await AppLog.Info($"end post-sql with result {postResult}");
                        }
                        trans.Commit();
                        await AppLog.Info($"transaction commited");
                        return result;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(beforeSql))
                        {
                            int prepareResult = conn.Execute(beforeSql, commandTimeout: commandTimeout);
                            await AppLog.Info($"end before command with result {prepareResult}");
                        }
                        int result = 0;
                        int counter = 0;
                        int take = 1000;
                        List<T> buffer = new List<T>();
                        foreach (var value in values)
                        {
                            buffer.Add(value);
                            if (buffer.Count == take)
                            {
                                try
                                {
                                    result += conn.Execute(insertTemplate,buffer);
                                    counter += take;
                                    await AppLog.Info($"inserted {counter} rows at {DateTime.UtcNow}");
                                    buffer = new List<T>();
                                }
                                catch (Exception ex)
                                {
                                    await AppLog.Error($"error inserting line [{counter}] of batch : {ex.Message}");
                                }
                            }
                        }
                        if (buffer.Any())
                        {
                            try
                            {
                                result += conn.Execute(insertTemplate, buffer);
                                counter += buffer.Count;
                                buffer = new List<T>();
                            }
                            catch (Exception ex)
                            {
                                await AppLog.Error($"error inserting line [{counter}] of batch : {ex.Message}");
                            }
                        }
                        await AppLog.Info($"end sql batch command, [{result}] lines inserted");

                        if (!string.IsNullOrEmpty(postSql))
                        {
                            int postResult = conn.Execute(postSql, commandTimeout: commandTimeout);
                            await AppLog.Info($"end post-sql with result {postResult}");
                        }
                        return result;
                    }

                }), this.connectionString);
    }
}