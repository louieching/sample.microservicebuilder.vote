package io.microprofile.showcase.vote.health;

import java.util.Optional;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.health.Health;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;

import io.microprofile.showcase.vote.api.SessionVote;

/**
 * 
 * @author jagraj
 *
 */
@Health
@ApplicationScoped
public class FailedHealthCheck implements HealthCheck{
	
	@Inject 
	private SessionVote sessionVote;
	
	@Inject HealthCheckBean healthCheckBean;

	@Inject 
	@ConfigProperty(name="isAppDown") Optional<Boolean> isAppDown;
    @Override
    public HealthCheckResponse call() {
		try {
			if(sessionVote.nessProbe().getStatus()!=200 || ((isAppDown.isPresent()) && (isAppDown.get().equals("true")))) {
				return HealthCheckResponse.named("Vote:failed-check").down().build();
			}
			else if(healthCheckBean.getIsAppDown()!=null && healthCheckBean.getIsAppDown().booleanValue()==true) {
				return HealthCheckResponse.named("Vote:failed-check").down().build();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return HealthCheckResponse.named("Vote:successful-check").up().build();
	}

}
