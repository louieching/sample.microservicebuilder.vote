/*
 * Copyright 2016 Microprofile.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.microprofile.showcase.vote.persistence.couch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.faulttolerance.Executor;
import org.eclipse.microprofile.faulttolerance.RetryPolicy;

import com.ibm.websphere.microprofile.faulttolerance.FaultToleranceFactory;

import io.microprofile.showcase.vote.api.PersistenceProvider;
import io.microprofile.showcase.vote.api.PersistenceTypes;
import io.microprofile.showcase.vote.model.SessionRating;
import io.microprofile.showcase.vote.persistence.Persistent;
import io.microprofile.showcase.vote.persistence.SessionRatingDAO;
import io.microprofile.showcase.vote.persistence.couch.CouchConnection.RequestType;
import io.microprofile.showcase.vote.utils.ConnectException;

@ApplicationScoped
@Persistent
public class CouchSessionRatingDAO implements SessionRatingDAO {

    @Inject
    CouchConnection couch;
    private @Inject PersistenceProvider persistenceProvider;
    private String dbName="ratings";

    private String allView = "function (doc) {emit(doc._id, 1)}";

    private String sessionView = "function (doc) {emit(doc.session, doc._id)}";
    private String attendeeView = "function (doc) {emit(doc.attendeeId, doc._id)}";

    private String designDoc = "{\"views\":{"
                               + "\"all\":{\"map\":\"" + allView + "\"},"
                               + "\"session\":{\"map\":\"" + sessionView + "\"},"
                               + "\"attendee\":{\"map\":\"" + attendeeView + "\"}}}";

    private boolean connected;
    private int executionCounter = 0;
    @PostConstruct
    public void connect() {
		RetryPolicy retryPolicy1 = FaultToleranceFactory.getFaultToleranceType(RetryPolicy.class);
		int delayDuration = 6;
		int maxRetries = 15;
		Duration duration = Duration.ofSeconds(delayDuration);
		retryPolicy1 = retryPolicy1.withDelay(duration).withMaxRetries(maxRetries).retryOn(ConnectException.class);
		// Create an Execution object. Configure it to connect to a "Primary",
		// with our RetryPolicy
		// and with a fallback to connect to a "HashMap"
		Executor executor = FaultToleranceFactory.getFaultToleranceType(Executor.class);

		// Main Service
		Callable<String> mainService = () -> {
			executionCounter++;
			System.out.println(
					"Delay Duration: " + delayDuration + " main Service called, execution " + executionCounter);
			this.connected = couch.connect(dbName);
			return null;
		};
		
		Consumer<String> successService = cxn -> {
			persistenceProvider.setPersistenceType(PersistenceTypes.PERSISTENCE_NO_SEQL_DB);
			persistenceProvider.setAvailable(true);
			
		};
		
		Callable<String> fallbackService=()-> {
			persistenceProvider.setPersistenceType(PersistenceTypes.NO_PERSISTENCE_HASH_MAP);
			persistenceProvider.setAvailable(true);
			// return new Boolean(this.connected);
			return null;

			
		};
		executor.with(retryPolicy1).onSuccess(successService).withFallback(fallbackService).get(mainService);
		
    	
		//couchFailsafe.couchConnectionWithFailSafe(dbName);
		if (persistenceProvider.getPersistenceType().equals(PersistenceTypes.PERSISTENCE_NO_SEQL_DB)
				&& persistenceProvider.isAvailable()) {
            String design = couch.request("_design/ratings", RequestType.GET, null, String.class, null, 200, true);
            if (design == null) {
                couch.request("_design/ratings", RequestType.PUT, designDoc, null, null, 201);
            }
        }else{
			this.connected=false;
		}
    }

    @Override
    public SessionRating rateSession(SessionRating sessionRating) {

        CouchID ratingID = couch.request(null, RequestType.POST, sessionRating, CouchID.class, null, 201);
        sessionRating = getSessionRating(ratingID.getId());

        return sessionRating;

    }

    private SessionRating getSessionRating(String id) {
        SessionRating sessionRating = couch.request(id, RequestType.GET, null, SessionRating.class, null, 200);
        return sessionRating;
    }

    @Override
    public SessionRating updateRating(SessionRating newRating) {
        SessionRating original = getSessionRating(newRating.getId());

        couch.request(newRating.getId(), RequestType.PUT, newRating, null, null, 201);

        newRating = getSessionRating(newRating.getId());
        return newRating;
    }

    @Override
    public void deleteRating(String id) {

        SessionRating original = getSessionRating(id);

        couch.request(id, RequestType.DELETE, null, null, null, 200);
    }

    @Override
    public SessionRating getRating(String id) {
        SessionRating sessionRating = couch.request(id, RequestType.GET, null, SessionRating.class, null, 200, true);
        return sessionRating;
    }

    @Override
    public Collection<SessionRating> getRatingsBySession(String sessionId) {
        return querySessionRating("session", sessionId);
    }

    @Override
    public Collection<SessionRating> getRatingsByAttendee(String attendeeId) {
        return querySessionRating("attendee", attendeeId);
    }

    private Collection<SessionRating> querySessionRating(String query, String value) {

        AllDocs allDocs = couch.request("_design/ratings/_view/" + query, "key", "\"" + value + "\"", RequestType.GET, null, AllDocs.class, null, 200);

        Collection<SessionRating> ratings = new ArrayList<SessionRating>();
        for (String id : allDocs.getIds()) {
            SessionRating rating = getSessionRating(id);
            ratings.add(rating);
        }

        return ratings;
    }

    @Override
    public Collection<SessionRating> getAllRatings() {

        AllDocs allDocs = couch.request("_design/ratings/_view/all", RequestType.GET, null, AllDocs.class, null, 200);

        Collection<SessionRating> sessionRatings = new ArrayList<SessionRating>();
        for (String id : allDocs.getIds()) {
            SessionRating sessionRating = getSessionRating(id);
            sessionRatings.add(sessionRating);
        }

        return sessionRatings;
    }

    @Override
    public void clearAllRatings() {
        AllDocs allDocs = couch.request("_design/ratings/_view/all", RequestType.GET, null, AllDocs.class, null, 200);

        for (String id : allDocs.getIds()) {
            deleteSessionRating(id);
        }

    }

    private void deleteSessionRating(String id) {
        SessionRating sessionRating = getSessionRating(id);

        couch.request(id, RequestType.DELETE, null, null, null, 200);
    }

}
