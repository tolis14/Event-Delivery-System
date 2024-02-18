import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/*
 * This class is responsible to update the stories
 * list. Stories expire after 1 minute for demostration
 * purposes so this thread is basically a deamon checking 
 * the stories that have expired and delete them.
 */
public class StoriesUpdateHandler implements Runnable{

	TimeUnit tu = TimeUnit.SECONDS;
	private final ConcurrentHashMap<String,ArrayList<Story>> stories;

	
	public StoriesUpdateHandler(ConcurrentHashMap<String,ArrayList<Story>> stories) {
		this.stories 		= stories;
	}
	
	@Override
	public synchronized void run() {
		// TODO Auto-generated method stub
		while(true) {
			ArrayList<Pair<String,ArrayList<Story>>> toBeDeleted = new ArrayList<Pair<String,ArrayList<Story>>>(); //stories that have expired.
			for (Map.Entry<String, ArrayList<Story>> set : this.stories.entrySet()) {
				Date dateNow = new Date();
				Pair<String,ArrayList<Story>> pair = new Pair<String,ArrayList<Story>>(set.getKey(),new ArrayList<Story>());
				for(Story s: set.getValue()) {
					
					long diffInSeconds = dateNow.getTime() - s.getDate().getTime();
					long seconds = tu.convert(diffInSeconds,TimeUnit.SECONDS);
					if(seconds>=60000) { //stories staying online for a minute. 
						pair.getValue().add(s);
					}
				}
				toBeDeleted.add(pair);	
			}
			for(Pair<String,ArrayList<Story>> p: toBeDeleted) {
				for(Story s: p.getValue()) {
					this.stories.get(p.getKey()).remove(s);
				}
			}
		}
	}
}
