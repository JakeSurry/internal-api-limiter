# Dave's problem: Rolling Q

## You have multiple different host that try to make an api call. Write a function called call api with rate limit, that allows for respecting api limits across multiples hosts, multiple memoory spaces, multiple processes. 

### API Rules for a given API:

```typescript
public static rateLimitRules: RateLimitRule[] = [
    // 15/s
    {
        userIdScope: true,
        ipScope: false,
        limit: 15,
        windowMs: 1_000,
    },
    // 500/min (equivalent to 8.33/s)
    {
        userIdScope: true,
        ipScope: false,
        limit: 500,
        windowMs: 60 * 1000,
    },
    // 16,500/hour (equivalent to 4.58/s)
    {
        userIdScope: true,
        ipScope: false,
        limit: 16_500,
        windowMs: 60 * 60 * 1000,
    },
];
```

### Interface for function
```typescript
export interface RateLimitRule {
    userIdScope: boolean; // true means user-wide, false means pms-wide ~~ HARD
    ipScope: boolean; // true means ip-wide, false means pms-wide ~~ HARD
    limit: number; // requests in
    windowMs: number; // ...this period
}
```

### Signature
```typescript
// signature
async function callApiWithRateLimit(api: string, userId: number, callApi: (...args: any[]): Promise<any>)
```

callApiWithRateLimit folllows RateLimitRule for  api, calls callApi when appropriate.

### Hint

Dave's solution took 2 weeks and was 250 lines in typescrpt.