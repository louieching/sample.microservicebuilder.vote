/**
 * 
 */
package io.microprofile.showcase.vote.health;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import org.eclipse.microprofile.health.Health;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;

import io.microprofile.showcase.vote.api.SessionVote;

/**
 * @author jagraj
 *
 */
@Health
@ApplicationScoped
public class SuccessfulHealthCheck implements HealthCheck {
	
	@Inject 
	private SessionVote sessionVote;
	@Override
	public HealthCheckResponse call() {
		try {
			if(sessionVote.nessProbe().getStatus()==200) {
				return HealthCheckResponse.named("Vote:successful-check").up().build();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return HealthCheckResponse.named("Vote:failed-check").down().build();
	}
}
