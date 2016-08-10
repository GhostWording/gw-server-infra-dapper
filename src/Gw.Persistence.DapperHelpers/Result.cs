using System;
using System.IO;
using System.Threading.Tasks;

namespace Gw.Persistence.DapperHelpers
{
 
    public class Unit : Result<Unit>
    {
        public Unit():base(Result<Unit>.None.Value)
        {
        }
    }
    public class Result<T>
    {
        public string Message => (Error?.Message) ?? "";

        public Exception Error { get; }
        public T Value { get; }
        public static Result<T> None { get { return default(Result<T>); } }

        public bool IsSuccess { get; }
        protected Result(T Value)
        {
            this.Value = Value;
            IsSuccess = true;
        }

        Result(Exception ex)
        {
            Error = ex;
            IsSuccess = false;
        }

        public static Result<T> New(Func<T> start)
            => SafeRun<T>()(start);

        public static Result<T> FromSuccess(T value)
            => new Result<T>(value);

        public static Result<T> FromFailure(Exception ex)
            => new Result<T>(ex);

        public static Result<T> FromFailure(string message)
            => new Result<T>(new ResultOperationException(message));

        public Result<TU> Then<TU>(Func<T, TU> onSuccess)
            => IsSuccess
                ? SafeContinue<TU>()(Value, onSuccess)
                : Result<TU>.FromFailure(Error);

        public TU Final<TU>(Func<T, TU> onSuccess, Func<Exception, TU> onError)
            => IsSuccess
                ? SafeResult<TU>()(Value, onSuccess)
                : onError(Error);

        public void Terminal(Action<T> onSuccess, Action<Exception> onError)
        {
            if (IsSuccess) onSuccess(Value);
            else onError(Error);
        }

        static Func<Func<TU>, Result<TU>> SafeRun<TU>()
            =>
                (start) =>
                {
                    try
                    {
                        return Result<TU>.FromSuccess(start());
                    }
                    catch (Exception ex)
                    {
                        return Result<TU>.FromFailure(ex);
                    }
                };

        static Func<T, Func<T, TU>, Result<TU>> SafeContinue<TU>()
            =>
                (source, next) =>
                {
                    try
                    {
                        return Result<TU>.FromSuccess(next(source));
                    }
                    catch (Exception ex)
                    {
                        return Result<TU>.FromFailure(ex);
                    }
                };

        static Func<T, Func<T, TU>, TU> SafeResult<TU>()
            =>
                (source, next) =>
                {
                    try
                    {
                        return next(source);
                    }
                    catch (Exception ex)
                    {
                        throw new FinalResultOperationException("Exception occur while extraction final result", ex);
                    }
                };
        

        public class ResultOperationException : Exception
        {
            public ResultOperationException(string message) : base(message)
            {
            }
        }
        public class FinalResultOperationException : Exception
        {
            public FinalResultOperationException(string message, Exception inner = null) : base(message, inner)
            {
            }
        }
    }
    

    public static class ResultExtensions
    {
        public static async Task<Result<Task<TU>>> Then<T, TU>(
            this Task<Result<T>> thisOperation, 
            Func<T, Task<TU>> onSuccess, Func<Exception,Task<Exception>> onError = null )
        {
            try
            {
                var result = await thisOperation;
                return result.IsSuccess 
                    ? Result<Task<TU>>.FromSuccess(onSuccess(result.Value)) 
                    : onError != null 
                        ? Result<Task<TU>>.FromFailure(onError(result.Error).Result)
                        : Result<Task<TU>>.FromFailure(result.Error);
            }
            catch (Exception ex)
            {
                return Result<Task<TU>>.FromFailure(ex);
            }
        }

        public static Result<T> GetResult<T>(this Task<Result<T>> thisOperation)
        {
            return thisOperation.Result;
        }
    }
}