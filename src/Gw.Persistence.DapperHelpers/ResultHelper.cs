namespace Gw.Persistence.DapperHelpers
{
    public static class ResultHelper
    {
        public static Result<T> ToResultSuccess<T>(this T current) => Result<T>.FromSuccess(current);
        public static Result<T> ToResultFailure<T>(this T current, string message) => Result<T>.FromFailure(message);
    }
}