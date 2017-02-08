// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* rest_entity.h                                                   -*- C++ -*-
   Jeremy Barnes, 8 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Basic definitions for entities accessible via REST.
*/

#pragma once

#include "mldb/types/date.h"
#include "mldb/types/string.h"
#include "mldb/watch/watch.h"
#include "link.h"
#include <map>


namespace MLDB {

/** Event that happens to a collection. */
enum CollectionEvent {
    CE_NEW,           ///< A new element was created
    CE_DELETED,       ///< An element was deleted
    CE_UPDATED,       ///< An element was updated
    CE_CONNECTED,     ///< We found an element through an existing peer
    CE_DISCONNECTED   ///< We lost an element because we disconnected from its peer
};

DECLARE_ENUM_DESCRIPTION(CollectionEvent);


struct RestEntity;
struct RestRequestRouter;
struct RestRequestParsingContext;

/*****************************************************************************/
/* RESOURCE PATH                                                             */
/*****************************************************************************/

/** Used to identify a resource. */

typedef std::vector<Utf8String> ResourcePath;

/** Used to specify a resource, including wildcards, etc. */

struct ResourceSpecEntry {
    ResourceSpecEntry()
    {
    }
    
    ResourceSpecEntry(const Utf8String & val)
    {
        parse(val);
    }

    ResourceSpecEntry(const std::string & val)
    {
        parse(Utf8String(val));
    }
    
    ResourceSpecEntry(const char * val)
    {
        parse(Utf8String(val));
    }

    ResourceSpecEntry(const Utf8String & channel,
                      const Utf8String & filter)
        : channel(channel), filter(filter)
    {
    }

    ResourceSpecEntry(const std::initializer_list<Utf8String> & il)
    {
        ExcAssertEqual(il.size(), 2);
        channel = *(il.begin() + 0);
        filter =  *(il.begin() + 1);
    }

    void parse(const Utf8String & val)
    {
        auto pos = val.find(':');
        if (pos == val.end()) {
            channel = "children";
            filter = val;
        }
        else {
            channel = Utf8String(val.begin(), pos);
            ++pos;
            filter = Utf8String(pos, val.end());
        }
    }

    Utf8String channel;     ///< Channel we're listening to
    Utf8String filter;      ///< How we filter out the channel

    Utf8String toString() const
    {
        if (channel == "children")
            return filter;
        else return channel + ":" + filter;
    }
};

inline std::ostream &
operator << (std::ostream & stream, const ResourceSpecEntry & entry)
{
    return stream << entry.toString();
}

DECLARE_STRUCTURE_DESCRIPTION(ResourceSpecEntry);


typedef std::vector<ResourceSpecEntry> ResourceSpec;

/// Watch result for a child watch
struct RestEntityChildEvent {
    RestEntityChildEvent()
        : parent(nullptr), entity(nullptr), object(nullptr)
    {
    }

    RestEntity * parent;  /// <Parent entity, or null
    Utf8String name;     /// <Name of the child
    RestEntity * entity;  /// <Child entity watched (set if convertible)
    void * object;        /// <Child object watched (always set)
    CollectionEvent event;
};

// Only a partial value description
DECLARE_STRUCTURE_DESCRIPTION(RestEntityChildEvent);

struct RestEntityChildEvents {
    std::vector<RestEntityChildEvent> children;
};

/*****************************************************************************/
/* ENTITY LINK STATUS                                                        */
/*****************************************************************************/

struct EntityLinkStatus {
    ResourcePath remoteAddress;
    LinkEnd end;
    Any connectParam;
    Any acceptParam;
    LinkState state;
    int messagesSent;
    int messagesReceived;
};

DECLARE_STRUCTURE_DESCRIPTION(EntityLinkStatus);


/*****************************************************************************/
/* ENTITY LINK TOKEN                                                         */
/*****************************************************************************/

/** A link token that knows which entities are at each of its ends. */

struct EntityLinkToken: public LinkToken {
    EntityLinkToken()
    {
    }

    EntityLinkToken(const ResourcePath & localAddress)
        : localAddress(localAddress)
    {
    }

    ResourcePath getLocalAddress() const
    {
        return localAddress;
    }

    void setLocalAddress(const ResourcePath & localAddress)
    {
        this->localAddress = localAddress;
    }

    ResourcePath getRemoteAddress() const
    {
        auto e = otherEnd();
        if (!e)
            return {};
        return e->getLocalAddress();
    }

    /** Return the status of the link. */
    EntityLinkStatus getStatus() const;

protected:
    std::shared_ptr<const EntityLinkToken> otherEnd() const
    {
        return std::static_pointer_cast<const EntityLinkToken>(LinkToken::otherEnd());
    }

    ResourcePath localAddress;
};


struct EntityLinks : public Links {
    EntityLinks()
    {
    }

    EntityLinks(const ResourcePath & localAddress)
        : localAddress(localAddress)
    {
    }

    void init(const ResourcePath & localAddress)
    {
        this->localAddress = localAddress;
    }

    std::shared_ptr<EntityLinkToken>
    accept(const ResourcePath & remoteAddress,
           Any connectParams = nullptr,
           Any acceptParams = nullptr)
    {
        std::shared_ptr<EntityLinkToken> connectEnd;
        std::shared_ptr<EntityLinkToken> acceptEnd;

        std::tie(connectEnd, acceptEnd)
            = createLinkT(std::unique_ptr<EntityLinkToken>(new EntityLinkToken(remoteAddress)),
                          std::unique_ptr<EntityLinkToken>(new EntityLinkToken(localAddress)),
                          LS_CONNECTED,
                          connectParams,
                          acceptParams);

        add(acceptEnd);
        return connectEnd;
    }

    std::vector<EntityLinkStatus> getStatus() const
    {
        std::vector<EntityLinkStatus> result;
        forEach([&] (const EntityLinkToken & tok) { result.push_back(tok.getStatus()); });
        return result;
    }

    void forEach(const std::function<void (EntityLinkToken & tok)> & fn)
    {
        auto fn2 = [&] (LinkToken & token)
            {
                fn(static_cast<EntityLinkToken &>(token));
            };
        Links::forEach(fn2);
    }

    void forEach(const std::function<void (const EntityLinkToken & tok)> & fn) const
    {
        auto fn2 = [&] (const LinkToken & token)
            {
                fn(static_cast<const EntityLinkToken &>(token));
            };
        Links::forEach(fn2);
    }

    ResourcePath localAddress;
};


/*****************************************************************************/
/* REST ENTITY                                                               */
/*****************************************************************************/

/** Base class for any entity that goes into REST. */

struct RestEntity {
    virtual ~RestEntity()
    {
    }

    /** Is this a collection of objects? */
    virtual bool isCollection() const = 0;

    /** Return a desciption of what this collection or entity holds */
    virtual Utf8String getDescription() const = 0;

    /** Get a watch on the given channel on the given path, including
        wildcards.
    */
    virtual Watch
    watch(const ResourceSpec & spec, bool catchUp, Any info = nullptr);

    /** Get a watch on the given channel on the given path, including
        wildcards.
    */
    virtual WatchT<std::vector<Utf8String>, Any>
    watchWithPath(const ResourceSpec & spec, bool catchUp, Any info = nullptr,
                  const std::vector<Utf8String> & currentPath
                       = std::vector<Utf8String>());

    template<typename... T>
    WatchT<std::vector<Utf8String>, T...>
    watchWithPathT(const ResourceSpec & spec, bool catchUp, Any info = nullptr,
                   const std::vector<Utf8String> & currentPath
                   = std::vector<Utf8String>())
    {
        return transform<std::vector<Utf8String>, T...>
            (watchWithPath(spec, catchUp, info, currentPath),
             [] (const std::vector<Utf8String> & path,
                 const Any & rest)
             {
                 return std::tuple_cat(std::tie(path),
                                       rest.as<std::tuple<T...> >());
             });
    }

    /** Return the ultimate type of a watch on the given path.

        Default throws saying it doesn't support watches.
    */
    virtual std::pair<const std::type_info *,
                      std::shared_ptr<const ValueDescription> >
    getWatchBoundType(const ResourceSpec & spec);

    /** Clear the children.  Only makes sense for collections. */
    virtual void clearChildren()
    {
    }

    /** Get a watch on a child or the children of the entity.  Default
        throws an exception as the entity cannot have children.
    */
    virtual WatchT<RestEntityChildEvent>
    watchChildren(const Utf8String & spec, bool catchUp, Any info);

    /** Get a watch on the given channel.  Default throws as no channels are
        supported.
    */
    virtual Watch watchChannel(const Utf8String & channel,
                               const Utf8String & filter,
                               bool catchUp,
                               Any info);

    /** Get our name within our parent collection. */
    virtual Utf8String getName() const = 0;

    /** Get our path in the REST hierarchy. */
    virtual std::vector<Utf8String> getPath() const;

    /** Get our parent. */
    virtual RestEntity * getParent() const = 0;

    /** Get a timer, optionally binding the given function in.  Default asks the
        parent for it. */
    virtual WatchT<Date> getTimer(Date nextExpiry, double period = -0.0,
                                  std::function<void (Date)> toBind = nullptr);

    /** Get the URI for the given path. */
    virtual Utf8String getUriForPath(ResourcePath path);

    /** Create a link with the given object.  Once the token disappears,
        the link will be broken too.
    */
    virtual std::shared_ptr<EntityLinkToken>
    createLink(const std::vector<Utf8String> & remotePath,
               const std::string & linkType,
               Any linkParams);

    /** Create a link with the given options. */
    template<typename... Args>
    std::shared_ptr<EntityLinkToken>
    createLinkT(const std::vector<Utf8String> & remotePath,
                const std::string & linkType,
                const Args&... args)
    {
        return createLink(remotePath, linkType,
                          Any(std::make_tuple(args...)));
    }

    /** Called to implement creating a link.  Default passes all the way
        back up to the top of the parent chain.
    */
    virtual std::shared_ptr<EntityLinkToken>
    doCreateLink(RestEntity * forWho,
                 const std::vector<Utf8String> & remotePath,
                 const std::string & linkType,
                 Any linkParams);

    /** Accept a link from another entity.  By default this will find the
        child with the first name in targetPath and recurse.
    */
    virtual std::shared_ptr<EntityLinkToken>
    acceptLink(const std::vector<Utf8String> & sourcePath,
               const std::vector<Utf8String> & targetPath,
               const std::string & linkType,
               Any linkParams);
};


/*****************************************************************************/
/* ROUTE MANAGER                                                             */
/*****************************************************************************/

/** Structure that manages a routing table for a tree of rest entities. */

struct RestRouteManager {
    RestRouteManager(RestRouteManager & parentManager,
                     RestRequestRouter & parentNode,
                     int resourceElementsMatched)
        : parentNode(&parentNode),
          resourceElementsMatched(resourceElementsMatched)
    {
    }

    // Called for the top level route
    RestRouteManager(RestRequestRouter & parentNode,
                     int resourceElementsMatched)
        : parentNode(&parentNode),
          resourceElementsMatched(resourceElementsMatched)
    {
    }


    RestRequestRouter * parentNode;

    /// The number of resource elements matched to get here.  Note that this
    /// is not the same as the length of the path, as regular expression
    /// matches may add more than one resource.
    ///
    /// For example, if this lives at /v1, it would be 1.  If it lives at
    /// /v1/collections/<collectionName:regex /([0-9a-zA-Z_]+)>, it would
    /// probably be 4, as the resources to get to /v1/collections/myCollection
    /// would be [ "/v1", "/collections", "/myCollection", "myCollection" ],
    /// with the 3rd element being the entire match of the regex, and the
    /// fourth the captured name of the collection.
    ///
    /// See the PathSpec and RestRequestParsingContext objects for more
    /// information.

    int resourceElementsMatched;

    std::map<Utf8String, std::shared_ptr<RestRouteManager> > childRoutes;

    /** Throw an exception that the given child is already added. */
    void throwChildAlreadyAdded(const Utf8String & name) MLDB_NORETURN;

    /** Add routes from a child. */
    template<typename Child, typename... Args>
    std::shared_ptr<typename Child::RouteManager>
    addChild(const Utf8String & name, Args&&... args)
    {
        if (childRoutes.count(name))
            throwChildAlreadyAdded(name);
            

        auto childRouteManager
            = std::make_shared<typename Child::RouteManager>
            (*this, std::forward<Args>(args)...);
        Child::initRoutes(*childRouteManager);
    
        // Save our child route
        childRoutes[name] = childRouteManager;
        
        return childRouteManager;
    }
};

/*****************************************************************************/
/* REST DIRECTORY                                                            */
/*****************************************************************************/

/** Something that is a collection of REST objects, each one with a different
    type and semantics.
*/

struct RestDirectory: public RestEntity {

    RestDirectory(RestEntity * parent, const Utf8String & name)
        : parent(parent), name(name)
    {
    }

    struct RouteManager: public RestRouteManager {

        typedef std::function<RestDirectory *
                              (const RestRequestParsingContext & context)>
        GetCollection;

        RouteManager(RestRouteManager & parentManager,
                     RestRequestRouter & parentNode,
                     int resourceElementsMatched,
                     GetCollection getCollection,
                     const Utf8String & nounSingular,
                     const Utf8String & nounPlural)
            : RestRouteManager(parentManager, parentNode,
                               resourceElementsMatched),
              getCollection(getCollection),
              collectionNode(nullptr),
              nounSingular(nounSingular),
              nounPlural(nounPlural)
             
        {
        }

        RouteManager(RestRequestRouter & parentNode,
                     int resourceElementsMatched,
                     GetCollection getCollection,
                     const Utf8String & nounSingular,
                     const Utf8String & nounPlural)
            : RestRouteManager(parentNode, resourceElementsMatched),
              getCollection(getCollection),
              collectionNode(nullptr),
              nounSingular(nounSingular),
              nounPlural(nounPlural)
        {
        }

        /// Function used to get the RestDirectory from the context
        GetCollection getCollection;

        /// Routes for the collection live here
        RestRequestRouter * collectionNode;

        Utf8String nounSingular;
        Utf8String nounPlural;
    };

    /** Initialize the nodes in the manager.  This can be called before
        initRoutes to control the creation of routes.
    */
    static void initNodes(RouteManager & manager);

    /** Initialize the routes in the manager. */
    static void initRoutes(RouteManager & manager);

    /** Add the given entity to the collection.  No ownership will be taken,
        so the caller guarantees that it will outlive the object.
    */
    void addEntity(Utf8String name, RestEntity & entity);

    /** Add the given entity to the collection.  Ownership is shared. */
    void addEntity(Utf8String name, std::shared_ptr<RestEntity> entity);

    virtual bool isCollection() const
    {
        return true;
    }

    /** Return a desciption of what this collection or entity holds */
    virtual Utf8String getDescription() const
    {
        return Utf8String("Operations");
    }

    /** Get a watch on a child or the children of the entity.  Default
        throws an exception as the entity cannot have children.
    */
    virtual WatchT<RestEntityChildEvent>
    watchChildren(const Utf8String & spec, bool catchUp, Any info);

    /** Get a watch on the given channel.  Default throws as no channels are
        supported.
    */
    virtual Watch watchChannel(const Utf8String & channel,
                               const Utf8String & filter,
                               bool catchUp, Any info);

    virtual std::pair<const std::type_info *,
                      std::shared_ptr<const ValueDescription> >
    getWatchBoundType(const ResourceSpec & spec);

    /** Clear the children.  Only makes sense for collections. */
    virtual void clearChildren()
    {
        entities.clear();
    }

    /** Get our path in the REST hierarchy. */
    virtual Utf8String getName() const
    {
        return name;
    }

    /** Get our parent. */
    virtual RestEntity * getParent() const
    {
        return parent;
    }

    virtual std::shared_ptr<EntityLinkToken>
    acceptLink(const std::vector<Utf8String> & sourcePath,
               const std::vector<Utf8String> & targetPath,
               const std::string & linkType,
               Any linkParams);

    virtual std::vector<Utf8String> getKeys() const
    {
        std::vector<Utf8String> result;
        for (auto & e: entities)
            result.push_back(e.first);
        return result;
    }

private:

    /// Entry containing all of the necessary information about an entity
    struct EntityEntry {
        EntityEntry(std::shared_ptr<RestEntity> entity)
            : entity(std::move(entity))
        {
        }

        WatchesT<RestEntityChildEvent> watches;
        std::shared_ptr<RestEntity> entity;
    };

    std::map<Utf8String, std::shared_ptr<EntityEntry> > entities;

    /// Parent entity for the collection
    RestEntity * parent;

    /// Name of our entry in the parent collection
    Utf8String name;
};


} // namespace MLDB
