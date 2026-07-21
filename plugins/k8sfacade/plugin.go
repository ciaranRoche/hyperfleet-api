// Package k8sfacade registers the Kubernetes-compatible read facade routes
// when k8s_facade.enabled is set. Routes are mounted on the root router —
// outside the /api/hyperfleet/v1 middleware chain — via RegisterRootRoutes.
package k8sfacade

import (
	"context"

	"github.com/gorilla/mux"

	"github.com/openshift-hyperfleet/hyperfleet-api/cmd/hyperfleet-api/environments"
	"github.com/openshift-hyperfleet/hyperfleet-api/cmd/hyperfleet-api/server"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/dao"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/k8sfacade"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/k8sfacade/cache"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/logger"
)

func init() {
	server.RegisterRootRoutes("k8sfacade", func(mainRouter *mux.Router, _ server.ServicesInterface) {
		env := environments.Environment()
		cfg := env.Config.K8sFacade
		if cfg == nil || !cfg.Enabled {
			return
		}

		ctx := context.Background()
		sessionFactory := env.Database.SessionFactory
		eventDao := dao.NewResourceEventDao(sessionFactory)

		feed := cache.NewFeed(eventDao, sessionFactory, cache.Config{
			PollInterval: cfg.PollInterval,
			Retention:    cfg.Retention,
			MinKeep:      cfg.RingSize,
		})
		if err := feed.Start(ctx); err != nil {
			// The facade is an optional read surface; a boot failure must not
			// take down the primary API. Watches will report unavailable.
			logger.WithError(ctx, err).Error("k8sfacade: failed to start watch feed")
			feed = nil
		}

		handler := k8sfacade.NewHandler(
			dao.NewResourceDao(sessionFactory),
			eventDao,
			feed,
			cfg.BookmarkInterval,
		)
		handler.RegisterRoutes(mainRouter)
		logger.Info(ctx, "Kubernetes-compatible facade enabled at /apis/"+k8sfacade.GroupVersion)
	})
}
