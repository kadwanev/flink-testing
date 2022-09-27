package flinkstreaming;

import org.apache.flink.queryablestate.client.QueryableStateClient;

public class AccountStateQuery {

    public static void main(String[] args) throws Exception {
        QueryableStateClient client = new QueryableStateClient("127.0.0.1", 9069);
/*  NK: unable to figure this out right now
        client.getKvState(JobID.fromHexString("5cbc6710b644ba30e7b082f3ba556dfc"),
                "accounts-state",
                402,
                TypeInformation.of(new TypeHint<Integer>() {}),
                new MapStateDescriptor<Integer, AccountMessage>("abcd"));

                .thenAccept(response -> {
                    try {
                        System.out.println(response.get(402));
                    }
                    catch (Exception ex) {
                        ex.printStackTrace();
                    }
                });
*/
//        CompletableFuture getKvState(
//                JobID jobId,
//                String queryableStateName,
//                K key,
//                TypeInformation<K> keyTypeInfo,
//                StateDescriptor<S, V> stateDescriptor)

    }

}
