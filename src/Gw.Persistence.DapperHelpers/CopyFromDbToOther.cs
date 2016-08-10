using System.Threading.Tasks;

namespace Gw.Persistence.DapperHelpers
{
    public class CopyFromDbToOther
    {
        private readonly SqlTasksHelper sourceDb;
        private readonly SqlTasksHelper targetDb;
        private readonly int defaultCommandTimeOut;
        public CopyFromDbToOther(
            string source, string target, int defaultCommandTimeOut)
        {
            this.sourceDb = new SqlTasksHelper(source);
            this.targetDb = new SqlTasksHelper(target);
            this.defaultCommandTimeOut = defaultCommandTimeOut;
        }

        public async Task<Result<int>> Copy(string select, string insert)
        {
            return
                (await
                    sourceDb
                        .Query<dynamic>(select, null, defaultCommandTimeOut)
                        .Then(async list => await targetDb.BatchInsert(
                            insert,
                            list,
                            commandTimeout: defaultCommandTimeOut))
                    ).Value.Result;
        }
        public async Task<Result<int>> Copy<T>(string select, string insert)
        {
            return
                (await
                    sourceDb
                        .Query<T>(select, null, defaultCommandTimeOut)
                        .Then(async list => await targetDb.BatchInsert(
                            insert,
                            list,
                            commandTimeout: defaultCommandTimeOut))
                    ).Value.Result;
        }
    }
}